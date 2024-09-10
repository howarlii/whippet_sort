

#pragma once

#include <cmath>
#include <memory>
#include <string>
#include <unordered_map>

#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_stream_utils.h"
// #include "arrow/util/byte_stream_split_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/ubsan.h"
#include "arrow/visit_data_inline.h"
#include "parquet/exception.h"
#include "parquet/platform.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include <arrow/io/api.h>
#include <arrow/result.h>
#include <arrow/util/bit_stream_utils.h>
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
using ::arrow::Status;
using ::arrow::VisitNullBitmapInline;
using ::arrow::internal::AddWithOverflow;
using ::arrow::internal::BitBlockCounter;
using ::arrow::internal::checked_cast;
using ::arrow::internal::MultiplyWithOverflow;
using ::arrow::internal::SafeSignedSubtract;
using ::arrow::internal::SubtractWithOverflow;
using ::arrow::util::SafeLoad;
using ::arrow::util::SafeLoadAs;
using std::string_view;

class DecoderImpl : virtual public Decoder {
public:
  void SetData(int num_values, const uint8_t *data, int len) override {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  int values_left() const override { return num_values_; }
  Encoding::type encoding() const override { return encoding_; }

protected:
  explicit DecoderImpl(const ColumnDescriptor *descr, Encoding::type encoding)
      : descr_(descr), encoding_(encoding), num_values_(0), data_(NULLPTR),
        len_(0) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor *descr_;

  const Encoding::type encoding_;
  int num_values_;
  const uint8_t *data_;
  int len_;
  int type_length_;
};

// ----------------------------------------------------------------------
// DeltaBitPackDecoder, copy from ARROW src/parquet/encoding.cc

template <typename DType>
class DeltaBitPackDecoder : public DecoderImpl,
                            virtual public TypedDecoder<DType> {
public:
  typedef typename DType::c_type T;
  using UT = std::make_unsigned_t<T>;

  explicit DeltaBitPackDecoder(
      const ColumnDescriptor *descr,
      MemoryPool *pool = ::arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BINARY_PACKED), pool_(pool) {
    if (DType::type_num != Type::INT32 && DType::type_num != Type::INT64) {
      throw ParquetException(
          "Delta bit pack encoding should only be for integer data.");
    }
  }

  void SetData(int num_values, const uint8_t *data, int len) override {
    // num_values is equal to page's num_values, including null values in this
    // page
    this->num_values_ = num_values;
    if (decoder_ == nullptr) {
      decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    } else {
      decoder_->Reset(data, len);
    }
    InitHeader();
  }

  // Set BitReader which is already initialized by DeltaLengthByteArrayDecoder
  // or DeltaByteArrayDecoder
  void SetDecoder(int num_values,
                  std::shared_ptr<::arrow::bit_util::BitReader> decoder) {
    this->num_values_ = num_values;
    decoder_ = std::move(decoder);
    InitHeader();
  }

  int ValidValuesCount() {
    // total_values_remaining_ in header ignores of null values
    return static_cast<int>(total_values_remaining_);
  }

  int Decode(T *buffer, int max_values) override {
    return GetInternal(buffer, max_values);
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator *out) override {
    if (null_count != 0) {
      // TODO(ARROW-34660): implement DecodeArrow with null slots.
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    int decoded_count = GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->AppendValues(values.data(), decoded_count));
    return decoded_count;
  }

  int DecodeArrow(
      int num_values, int null_count, const uint8_t *valid_bits,
      int64_t valid_bits_offset,
      typename EncodingTraits<DType>::DictAccumulator *out) override {
    if (null_count != 0) {
      // TODO(ARROW-34660): implement DecodeArrow with null slots.
      ParquetException::NYI("Delta bit pack DecodeArrow with null slots");
    }
    std::vector<T> values(num_values);
    int decoded_count = GetInternal(values.data(), num_values);
    PARQUET_THROW_NOT_OK(out->Reserve(decoded_count));
    for (int i = 0; i < decoded_count; ++i) {
      PARQUET_THROW_NOT_OK(out->Append(values[i]));
    }
    return decoded_count;
  }

private:
  static constexpr int kMaxDeltaBitWidth = static_cast<int>(sizeof(T) * 8);

  void InitHeader() {
    if (!decoder_->GetVlqInt(&values_per_block_) ||
        !decoder_->GetVlqInt(&mini_blocks_per_block_) ||
        !decoder_->GetVlqInt(&total_value_count_) ||
        !decoder_->GetZigZagVlqInt(&last_value_)) {
      ParquetException::EofException("InitHeader EOF");
    }

    if (values_per_block_ == 0) {
      throw ParquetException("cannot have zero value per block");
    }
    if (values_per_block_ % 128 != 0) {
      throw ParquetException(
          "the number of values in a block must be multiple of 128, but it's " +
          std::to_string(values_per_block_));
    }
    if (mini_blocks_per_block_ == 0) {
      throw ParquetException("cannot have zero miniblock per block");
    }
    values_per_mini_block_ = values_per_block_ / mini_blocks_per_block_;
    if (values_per_mini_block_ == 0) {
      throw ParquetException("cannot have zero value per miniblock");
    }
    if (values_per_mini_block_ % 32 != 0) {
      throw ParquetException("the number of values in a miniblock must be "
                             "multiple of 32, but it's " +
                             std::to_string(values_per_mini_block_));
    }

    total_values_remaining_ = total_value_count_;
    if (delta_bit_widths_ == nullptr) {
      delta_bit_widths_ = AllocateBuffer(pool_, mini_blocks_per_block_);
    } else {
      PARQUET_THROW_NOT_OK(delta_bit_widths_->Resize(mini_blocks_per_block_,
                                                     /*shrink_to_fit*/ false));
    }
    first_block_initialized_ = false;
    values_remaining_current_mini_block_ = 0;
  }

  void InitBlock() {
    DCHECK_GT(total_values_remaining_, 0) << "InitBlock called at EOF";

    if (!decoder_->GetZigZagVlqInt(&min_delta_))
      ParquetException::EofException("InitBlock EOF");

    // read the bitwidth of each miniblock
    uint8_t *bit_width_data = delta_bit_widths_->mutable_data();
    for (uint32_t i = 0; i < mini_blocks_per_block_; ++i) {
      if (!decoder_->GetAligned<uint8_t>(1, bit_width_data + i)) {
        ParquetException::EofException("Decode bit-width EOF");
      }
      // Note that non-conformant bitwidth entries are allowed by the Parquet
      // spec for extraneous miniblocks in the last block (GH-14923), so we
      // check the bitwidths when actually using them (see InitMiniBlock()).
    }

    mini_block_idx_ = 0;
    first_block_initialized_ = true;
    InitMiniBlock(bit_width_data[0]);
  }

  void InitMiniBlock(int bit_width) {
    if (ARROW_PREDICT_FALSE(bit_width > kMaxDeltaBitWidth)) {
      throw ParquetException("delta bit width larger than integer bit width");
    }
    delta_bit_width_ = bit_width;
    values_remaining_current_mini_block_ = values_per_mini_block_;
  }

  int GetInternal(T *buffer, int max_values) {
    max_values = static_cast<int>(
        std::min<int64_t>(max_values, total_values_remaining_));
    if (max_values == 0) {
      return 0;
    }

    int i = 0;

    if (ARROW_PREDICT_FALSE(!first_block_initialized_)) {
      // This is the first time we decode this data page, first output the
      // last value and initialize the first block.
      buffer[i++] = last_value_;
      if (ARROW_PREDICT_FALSE(i == max_values)) {
        // When i reaches max_values here we have two different possibilities:
        // 1. total_value_count_ == 1, which means that the page may have only
        //    one value (encoded in the header), and we should not initialize
        //    any block, nor should we skip any padding bits below.
        // 2. total_value_count_ != 1, which means we should initialize the
        //    incoming block for subsequent reads.
        if (total_value_count_ != 1) {
          InitBlock();
        }
        total_values_remaining_ -= max_values;
        this->num_values_ -= max_values;
        return max_values;
      }
      InitBlock();
    }

    DCHECK(first_block_initialized_);
    while (i < max_values) {
      // Ensure we have an initialized mini-block
      if (ARROW_PREDICT_FALSE(values_remaining_current_mini_block_ == 0)) {
        ++mini_block_idx_;
        if (mini_block_idx_ < mini_blocks_per_block_) {
          InitMiniBlock(delta_bit_widths_->data()[mini_block_idx_]);
        } else {
          InitBlock();
        }
      }

      int values_decode = std::min(values_remaining_current_mini_block_,
                                   static_cast<uint32_t>(max_values - i));
      if (decoder_->GetBatch(delta_bit_width_, buffer + i, values_decode) !=
          values_decode) {
        ParquetException::EofException();
      }
      for (int j = 0; j < values_decode; ++j) {
        // Addition between min_delta, packed int and last_value should be
        // treated as unsigned addition. Overflow is as expected.
        buffer[i + j] = static_cast<UT>(min_delta_) +
                        static_cast<UT>(buffer[i + j]) +
                        static_cast<UT>(last_value_);
        last_value_ = buffer[i + j];
      }
      values_remaining_current_mini_block_ -= values_decode;
      i += values_decode;
    }
    total_values_remaining_ -= max_values;
    this->num_values_ -= max_values;

    if (ARROW_PREDICT_FALSE(total_values_remaining_ == 0)) {
      uint32_t padding_bits =
          values_remaining_current_mini_block_ * delta_bit_width_;
      // skip the padding bits
      if (!decoder_->Advance(padding_bits)) {
        ParquetException::EofException();
      }
      values_remaining_current_mini_block_ = 0;
    }
    return max_values;
  }

  MemoryPool *pool_;
  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  uint32_t values_per_block_;
  uint32_t mini_blocks_per_block_;
  uint32_t values_per_mini_block_;
  uint32_t total_value_count_;

  uint32_t total_values_remaining_;
  // Remaining values in current mini block. If the current block is the last
  // mini block, values_remaining_current_mini_block_ may greater than
  // total_values_remaining_.
  uint32_t values_remaining_current_mini_block_;

  // If the page doesn't contain any block, `first_block_initialized_` will
  // always be false. Otherwise, it will be true when first block initialized.
  bool first_block_initialized_;
  T min_delta_;
  uint32_t mini_block_idx_;
  std::shared_ptr<ResizableBuffer> delta_bit_widths_;
  int delta_bit_width_;

  T last_value_;
};

// ----------------------------------------------------------------------
// DeltaByteArraySortDecoder, a hack class for sort
template <typename DType>
class DeltaByteArraySortDecoder : public DecoderImpl,
                                  virtual public TypedDecoder<DType> {
  using T = typename DType::c_type;

public:
  explicit DeltaByteArraySortDecoder(
      const ColumnDescriptor *descr,
      MemoryPool *pool = ::arrow::default_memory_pool())
      : DecoderImpl(descr, Encoding::DELTA_BYTE_ARRAY), pool_(pool),
        prefix_len_decoder_(nullptr, pool),
        suffix_decoder_(std::make_unique<TypedDecoder<ByteArrayType>>(
            MakeDecoder(Type::BYTE_ARRAY, Encoding::DELTA_LENGTH_BYTE_ARRAY,
                        nullptr, pool)
                .release())),
        last_value_in_previous_page_(""),
        buffered_prefix_length_(AllocateBuffer(pool, 0)),
        buffered_data_(AllocateBuffer(pool, 0)) {}

  void SetData(int num_values, const uint8_t *data, int len) override {
    num_values_ = num_values;
    if (decoder_) {
      decoder_->Reset(data, len);
    } else {
      decoder_ = std::make_shared<::arrow::bit_util::BitReader>(data, len);
    }
    prefix_len_decoder_.SetDecoder(num_values, decoder_);

    // get the number of encoded prefix lengths
    int num_prefix = prefix_len_decoder_.ValidValuesCount();
    // call prefix_len_decoder_.Decode to decode all the prefix lengths.
    // all the prefix lengths are buffered in buffered_prefix_length_.
    PARQUET_THROW_NOT_OK(
        buffered_prefix_length_->Resize(num_prefix * sizeof(int32_t)));
    int ret = prefix_len_decoder_.Decode(
        buffered_prefix_length_->mutable_data_as<int32_t>(), num_prefix);
    DCHECK_EQ(ret, num_prefix);
    prefix_len_offset_ = 0;
    num_valid_values_ = num_prefix;

    int bytes_left = decoder_->bytes_left();
    // If len < bytes_left, prefix_len_decoder.Decode will throw exception.
    DCHECK_GE(len, bytes_left);
    int suffix_begins = len - bytes_left;
    // at this time, the decoder_ will be at the start of the encoded suffix
    // data.
    suffix_decoder_->SetData(num_values, data + suffix_begins, bytes_left);

    // TODO: read corrupted files written with bug(PARQUET-246). last_value_
    // should be set to last_value_in_previous_page_ when decoding a new
    // page(except the first page)
    last_value_.clear();
  }

  int DecodeArrow(int num_values, int null_count, const uint8_t *valid_bits,
                  int64_t valid_bits_offset,
                  typename EncodingTraits<DType>::Accumulator *out) override {
    int result = 0;
    PARQUET_THROW_NOT_OK(DecodeArrowDense(num_values, null_count, valid_bits,
                                          valid_bits_offset, out, &result));
    return result;
  }

  int DecodeArrow(
      int num_values, int null_count, const uint8_t *valid_bits,
      int64_t valid_bits_offset,
      typename EncodingTraits<DType>::DictAccumulator *builder) override {
    ParquetException::NYI(
        "DecodeArrow of DictAccumulator for DeltaByteArrayDecoder");
  }

protected:
  template <bool is_first_run>
  static void BuildBufferInternal(const int32_t *prefix_len_ptr, int i,
                                  ByteArray *buffer, std::string_view *prefix,
                                  uint8_t **data_ptr) {
    if (ARROW_PREDICT_FALSE(static_cast<size_t>(prefix_len_ptr[i]) >
                            prefix->length())) {
      throw ParquetException("prefix length too large in DELTA_BYTE_ARRAY");
    }
    // For now, `buffer` points to string suffixes, and the suffix decoder
    // ensures that the suffix data has sufficient lifetime.
    if (prefix_len_ptr[i] == 0) {
      // prefix is empty: buffer[i] already points to the suffix.
      *prefix = std::string_view{buffer[i]};
      return;
    }
    DCHECK_EQ(is_first_run, i == 0);
    if constexpr (!is_first_run) {
      if (buffer[i].len == 0) {
        // suffix is empty: buffer[i] can simply point to the prefix.
        // This is not possible for the first run since the prefix
        // would point to the mutable `last_value_`.
        *prefix = prefix->substr(0, prefix_len_ptr[i]);
        buffer[i] = ByteArray(*prefix);
        return;
      }
    }
    // Both prefix and suffix are non-empty, so we need to decode the string
    // into `data_ptr`.
    // 1. Copy the prefix
    memcpy(*data_ptr, prefix->data(), prefix_len_ptr[i]);
    // 2. Copy the suffix.
    memcpy(*data_ptr + prefix_len_ptr[i], buffer[i].ptr, buffer[i].len);
    // 3. Point buffer[i] to the decoded string.
    buffer[i].ptr = *data_ptr;
    buffer[i].len += prefix_len_ptr[i];
    *data_ptr += buffer[i].len;
    *prefix = std::string_view{buffer[i]};
  }

  int GetInternal(ByteArray *buffer, int max_values) {
    // Decode up to `max_values` strings into an internal buffer
    // and reference them into `buffer`.
    max_values = std::min(max_values, num_valid_values_);
    if (max_values == 0) {
      return max_values;
    }

    int suffix_read = suffix_decoder_->Decode(buffer, max_values);
    if (ARROW_PREDICT_FALSE(suffix_read != max_values)) {
      ParquetException::EofException(
          "Read " + std::to_string(suffix_read) + ", expecting " +
          std::to_string(max_values) + " from suffix decoder");
    }

    int64_t data_size = 0;
    const int32_t *prefix_len_ptr =
        buffered_prefix_length_->data_as<int32_t>() + prefix_len_offset_;
    for (int i = 0; i < max_values; ++i) {
      if (prefix_len_ptr[i] == 0) {
        // We don't need to copy the suffix if the prefix length is 0.
        continue;
      }
      if (ARROW_PREDICT_FALSE(prefix_len_ptr[i] < 0)) {
        throw ParquetException("negative prefix length in DELTA_BYTE_ARRAY");
      }
      if (buffer[i].len == 0 && i != 0) {
        // We don't need to copy the prefix if the suffix length is 0
        // and this is not the first run (that is, the prefix doesn't point
        // to the mutable `last_value_`).
        continue;
      }
      if (ARROW_PREDICT_FALSE(
              AddWithOverflow(data_size, prefix_len_ptr[i], &data_size) ||
              AddWithOverflow(data_size, buffer[i].len, &data_size))) {
        throw ParquetException("excess expansion in DELTA_BYTE_ARRAY");
      }
    }
    PARQUET_THROW_NOT_OK(buffered_data_->Resize(data_size));

    string_view prefix{last_value_};
    uint8_t *data_ptr = buffered_data_->mutable_data();
    if (max_values > 0) {
      BuildBufferInternal</*is_first_run=*/true>(prefix_len_ptr, 0, buffer,
                                                 &prefix, &data_ptr);
    }
    for (int i = 1; i < max_values; ++i) {
      BuildBufferInternal</*is_first_run=*/false>(prefix_len_ptr, i, buffer,
                                                  &prefix, &data_ptr);
    }
    DCHECK_EQ(data_ptr - buffered_data_->mutable_data(), data_size);
    prefix_len_offset_ += max_values;
    this->num_values_ -= max_values;
    num_valid_values_ -= max_values;
    last_value_ = std::string{prefix};

    if (num_valid_values_ == 0) {
      last_value_in_previous_page_ = last_value_;
    }
    return max_values;
  }

  // Status DecodeArrowDense(int num_values, int null_count,
  //                         const uint8_t *valid_bits, int64_t
  //                         valid_bits_offset, typename
  //                         EncodingTraits<DType>::Accumulator *out, int
  //                         *out_num_values) {
  //   ArrowBinaryHelper<DType> helper(out, num_values);
  //   RETURN_NOT_OK(helper.Prepare());

  //   std::vector<ByteArray> values(num_values);
  //   const int num_valid_values =
  //       GetInternal(values.data(), num_values - null_count);
  //   DCHECK_EQ(num_values - null_count, num_valid_values);

  //   auto values_ptr = reinterpret_cast<const ByteArray *>(values.data());
  //   int value_idx = 0;

  //   RETURN_NOT_OK(VisitNullBitmapInline(
  //       valid_bits, valid_bits_offset, num_values, null_count,
  //       [&]() {
  //         const auto &val = values_ptr[value_idx];
  //         RETURN_NOT_OK(helper.PrepareNextInput(val.len));
  //         RETURN_NOT_OK(helper.Append(val.ptr,
  //         static_cast<int32_t>(val.len)));
  //         ++value_idx;
  //         return Status::OK();
  //       },
  //       [&]() {
  //         RETURN_NOT_OK(helper.AppendNull());
  //         --null_count;
  //         return Status::OK();
  //       }));

  //   DCHECK_EQ(null_count, 0);
  //   *out_num_values = num_valid_values;
  //   return Status::OK();
  // }

  MemoryPool *pool_;

private:
  std::shared_ptr<::arrow::bit_util::BitReader> decoder_;
  DeltaBitPackDecoder<Int32Type> prefix_len_decoder_;
  std::unique_ptr<TypedDecoder<ByteArrayType>> suffix_decoder_;
  std::string last_value_;
  // string buffer for last value in previous page
  std::string last_value_in_previous_page_;
  int num_valid_values_{0};
  uint32_t prefix_len_offset_{0};
  std::shared_ptr<ResizableBuffer> buffered_prefix_length_;
  std::shared_ptr<ResizableBuffer> buffered_data_;
};
} // namespace whippet_sort::hack_parquet