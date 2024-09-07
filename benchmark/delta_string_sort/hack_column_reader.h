

#pragma once

#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>

#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/util/logging.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <parquet/arrow/reader.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/file_reader.h>
#include <parquet/types.h>

namespace whippet_sort::hack_parquet {

using namespace parquet;

static bool IsDictionaryIndexEncoding(const Encoding::type &e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <typename DType> class ColumnReaderImplBase {
public:
  using T = typename DType::c_type;

  ColumnReaderImplBase(const ColumnDescriptor *descr, ::arrow::MemoryPool *pool)
      : descr_(descr), max_def_level_(descr->max_definition_level()),
        max_rep_level_(descr->max_repetition_level()), num_buffered_values_(0),
        num_decoded_values_(0), pool_(pool), current_decoder_(nullptr),
        current_encoding_(Encoding::UNKNOWN) {}

  virtual ~ColumnReaderImplBase() = default;

protected:
  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T *out) {
    int64_t num_decoded =
        current_decoder_->Decode(out, static_cast<int>(batch_size));
    return num_decoded;
  }

  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*, leaving spaces for null entries according
  // to the def_levels.
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValuesSpaced(int64_t batch_size, T *out, int64_t null_count,
                           uint8_t *valid_bits, int64_t valid_bits_offset) {
    return current_decoder_->DecodeSpaced(out, static_cast<int>(batch_size),
                                          static_cast<int>(null_count),
                                          valid_bits, valid_bits_offset);
  }

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  int64_t ReadDefinitionLevels(int64_t batch_size, int16_t *levels) {
    if (max_def_level_ == 0) {
      return 0;
    }
    return definition_level_decoder_.Decode(static_cast<int>(batch_size),
                                            levels);
  }

  bool HasNextInternal() {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (num_buffered_values_ == 0 ||
        num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  // Read multiple repetition levels into preallocated memory
  // Returns the number of decoded repetition levels
  int64_t ReadRepetitionLevels(int64_t batch_size, int16_t *levels) {
    if (max_rep_level_ == 0) {
      return 0;
    }
    return repetition_level_decoder_.Decode(static_cast<int>(batch_size),
                                            levels);
  }

  // Advance to the next data page
  bool ReadNewPage() {
    // Loop until we find the next data page.
    while (true) {
      current_page_ = pager_->NextPage();
      if (!current_page_) {
        // EOS
        return false;
      }

      if (current_page_->type() == PageType::DICTIONARY_PAGE) {
        ConfigureDictionary(
            static_cast<const DictionaryPage *>(current_page_.get()));
        continue;
      } else if (current_page_->type() == PageType::DATA_PAGE) {
        const auto page = std::static_pointer_cast<DataPageV1>(current_page_);
        const int64_t levels_byte_size =
            InitializeLevelDecoders(*page, page->repetition_level_encoding(),
                                    page->definition_level_encoding());
        InitializeDataDecoder(*page, levels_byte_size);
        return true;
      } else if (current_page_->type() == PageType::DATA_PAGE_V2) {
        const auto page = std::static_pointer_cast<DataPageV2>(current_page_);
        int64_t levels_byte_size = InitializeLevelDecodersV2(*page);
        InitializeDataDecoder(*page, levels_byte_size);
        return true;
      } else {
        // We don't know what this page type is. We're allowed to skip non-data
        // pages.
        continue;
      }
    }
    return true;
  }

  void ConfigureDictionary(const DictionaryPage *page) {
    int encoding = static_cast<int>(page->encoding());
    if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
        page->encoding() == Encoding::PLAIN) {
      encoding = static_cast<int>(Encoding::RLE_DICTIONARY);
    }

    auto it = decoders_.find(encoding);
    if (it != decoders_.end()) {
      throw ParquetException("Column cannot have more than one dictionary.");
    }

    if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
        page->encoding() == Encoding::PLAIN) {
      auto dictionary = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
      dictionary->SetData(page->num_values(), page->data(), page->size());

      // The dictionary is fully decoded during DictionaryDecoder::Init, so the
      // DictionaryPage buffer is no longer required after this step
      //
      // TODO(wesm): investigate whether this all-or-nothing decoding of the
      // dictionary makes sense and whether performance can be improved

      std::unique_ptr<DictDecoder<DType>> decoder =
          MakeDictDecoder<DType>(descr_, pool_);
      decoder->SetDict(dictionary.get());
      decoders_[encoding] = std::unique_ptr<DecoderType>(
          dynamic_cast<DecoderType *>(decoder.release()));
    } else {
      ParquetException::NYI(
          "only plain dictionary encoding has been implemented");
    }

    new_dictionary_ = true;
    current_decoder_ = decoders_[encoding].get();
    ARROW_DCHECK(current_decoder_);
  }

  // Initialize repetition and definition level decoders on the next data page.

  // If the data page includes repetition and definition levels, we
  // initialize the level decoders and return the number of encoded level bytes.
  // The return value helps determine the number of bytes in the encoded data.
  int64_t InitializeLevelDecoders(const DataPage &page,
                                  Encoding::type repetition_level_encoding,
                                  Encoding::type definition_level_encoding) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;

    const uint8_t *buffer = page.data();
    int32_t levels_byte_size = 0;
    int32_t max_size = page.size();

    // Data page Layout: Repetition Levels - Definition Levels - encoded values.
    // Levels are encoded as rle or bit-packed.
    // Init repetition levels
    if (max_rep_level_ > 0) {
      int32_t rep_levels_bytes = repetition_level_decoder_.SetData(
          repetition_level_encoding, max_rep_level_,
          static_cast<int>(num_buffered_values_), buffer, max_size);
      buffer += rep_levels_bytes;
      levels_byte_size += rep_levels_bytes;
      max_size -= rep_levels_bytes;
    }
    // TODO figure a way to set max_def_level_ to 0
    // if the initial value is invalid

    // Init definition levels
    if (max_def_level_ > 0) {
      int32_t def_levels_bytes = definition_level_decoder_.SetData(
          definition_level_encoding, max_def_level_,
          static_cast<int>(num_buffered_values_), buffer, max_size);
      levels_byte_size += def_levels_bytes;
      max_size -= def_levels_bytes;
    }

    return levels_byte_size;
  }

  int64_t InitializeLevelDecodersV2(const DataPageV2 &page) {
    // Read a data page.
    num_buffered_values_ = page.num_values();

    // Have not decoded any values from the data page yet
    num_decoded_values_ = 0;
    const uint8_t *buffer = page.data();

    const int64_t total_levels_length =
        static_cast<int64_t>(page.repetition_levels_byte_length()) +
        page.definition_levels_byte_length();

    if (total_levels_length > page.size()) {
      throw ParquetException(
          "Data page too small for levels (corrupt header?)");
    }

    if (max_rep_level_ > 0) {
      repetition_level_decoder_.SetDataV2(
          page.repetition_levels_byte_length(), max_rep_level_,
          static_cast<int>(num_buffered_values_), buffer);
    }
    // ARROW-17453: Even if max_rep_level_ is 0, there may still be
    // repetition level bytes written and/or reported in the header by
    // some writers (e.g. Athena)
    buffer += page.repetition_levels_byte_length();

    if (max_def_level_ > 0) {
      definition_level_decoder_.SetDataV2(
          page.definition_levels_byte_length(), max_def_level_,
          static_cast<int>(num_buffered_values_), buffer);
    }

    return total_levels_length;
  }

  // Get a decoder object for this page or create a new decoder if this is the
  // first page with this encoding.
  void InitializeDataDecoder(const DataPage &page, int64_t levels_byte_size) {
    const uint8_t *buffer = page.data() + levels_byte_size;
    const int64_t data_size = page.size() - levels_byte_size;

    if (data_size < 0) {
      throw ParquetException("Page smaller than size of encoded levels");
    }

    Encoding::type encoding = page.encoding();

    if (IsDictionaryIndexEncoding(encoding)) {
      encoding = Encoding::RLE_DICTIONARY;
    }

    auto it = decoders_.find(static_cast<int>(encoding));
    if (it != decoders_.end()) {
      ARROW_DCHECK(it->second.get() != nullptr);
      current_decoder_ = it->second.get();
    } else {
      switch (encoding) {
      case Encoding::PLAIN: {
        auto decoder = MakeTypedDecoder<DType>(Encoding::PLAIN, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }
      case Encoding::BYTE_STREAM_SPLIT: {
        auto decoder =
            MakeTypedDecoder<DType>(Encoding::BYTE_STREAM_SPLIT, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }
      case Encoding::RLE: {
        auto decoder = MakeTypedDecoder<DType>(Encoding::RLE, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }
      case Encoding::RLE_DICTIONARY:
        throw ParquetException("Dictionary page must be before data page.");

      case Encoding::DELTA_BINARY_PACKED: {
        auto decoder =
            MakeTypedDecoder<DType>(Encoding::DELTA_BINARY_PACKED, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }
      case Encoding::DELTA_BYTE_ARRAY: {
        auto decoder =
            MakeTypedDecoder<DType>(Encoding::DELTA_BYTE_ARRAY, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }
      case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
        auto decoder =
            MakeTypedDecoder<DType>(Encoding::DELTA_LENGTH_BYTE_ARRAY, descr_);
        current_decoder_ = decoder.get();
        decoders_[static_cast<int>(encoding)] = std::move(decoder);
        break;
      }

      default:
        throw ParquetException("Unknown encoding type.");
      }
    }
    current_encoding_ = encoding;
    current_decoder_->SetData(static_cast<int>(num_buffered_values_), buffer,
                              static_cast<int>(data_size));
  }

  int64_t available_values_current_page() const {
    return num_buffered_values_ - num_decoded_values_;
  }

  const ColumnDescriptor *descr_;
  const int16_t max_def_level_;
  const int16_t max_rep_level_;

  std::unique_ptr<PageReader> pager_;
  std::shared_ptr<Page> current_page_;

  // Not set if full schema for this field has no optional or repeated elements
  LevelDecoder definition_level_decoder_;

  // Not set for flat schemas.
  LevelDecoder repetition_level_decoder_;

  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int64_t num_buffered_values_;

  // The number of values from the current data page that have been decoded
  // into memory
  int64_t num_decoded_values_;

  ::arrow::MemoryPool *pool_;

  using DecoderType = TypedDecoder<DType>;
  DecoderType *current_decoder_;
  Encoding::type current_encoding_;

  /// Flag to signal when a new dictionary has been set, for the benefit of
  /// DictionaryRecordReader
  bool new_dictionary_;

  // The exposed encoding
  ExposedEncoding exposed_encoding_ = ExposedEncoding::NO_ENCODING;

  // Map of encoding type to the respective decoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::unique_ptr<DecoderType>> decoders_;

  void ConsumeBufferedValues(int64_t num_values) {
    num_decoded_values_ += num_values;
  }
};

template <typename DType>
class TypedColumnReaderImpl : public ColumnReaderImplBase<DType> {
public:
  using T = typename DType::c_type;

  TypedColumnReaderImpl(const ColumnDescriptor *descr,
                        std::unique_ptr<PageReader> pager,
                        ::arrow::MemoryPool *pool)
      : ColumnReaderImplBase<DType>(descr, pool) {
    this->pager_ = std::move(pager);
  }

  bool HasNext() { return this->HasNextInternal(); }

  //   using ColumnReaderImplBase<DType>::ReadValues;
  // Read up to batch_size values from the current data page into the
  // pre-allocated memory T*
  //
  // @returns: the number of values read into the out buffer
  int64_t ReadValues(int64_t batch_size, T *out) {
    if (!HasNext()) {
      return 0;
    }
    auto values_read = ColumnReaderImplBase<DType>::ReadValues(batch_size, out);
    this->ConsumeBufferedValues(values_read);
    return values_read;
  }
};
} // namespace whippet_sort::hack_parquet