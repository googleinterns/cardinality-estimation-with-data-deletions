/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef THETA_SKETCH_DUP_H_
#define THETA_SKETCH_DUP_H_

#include <algorithm>
#include <climits>
#include <cmath>
#include <functional>
#include <istream>
#include <memory>
#include <ostream>
#include <sstream>
#include <cstring>
#include <vector>

#include "utils.h"

#include "MurmurHash3.h"
#include "binomial_bounds.hpp"
#include "common_defs.hpp"
#include "memory_operations.hpp"
#include "serde.hpp"

namespace datasketches {

/*
 * theta_sketch_dup is modified from theta_sketch of
 * https://github.com/apache/incubator-datasketches-cpp/tree/master/theta
 * compared with theta_sketch, theta_sketch_dup enables processing duplicate
 * elements as well as deleting elements
 */

/*
 * The following are declarations
 */

// forward-declarations
template <typename A>
class theta_sketch_dup_alloc;
template <typename A>
class update_theta_sketch_dup_alloc;
template <typename A>
class compact_theta_sketch_dup_alloc;

/* TODO: support set operations
 * template<typename A> class theta_union_dup_alloc;
 * template<typename A> class theta_intersection_dup_alloc;
 * template<typename A> class theta_a_not_b_dup_alloc;
 */

// for serialization as raw bytes
template <typename A>
using AllocU8 =
    typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template <typename A>
using vector_u8 = std::vector<uint8_t, AllocU8<A>>;

template <typename A>
class theta_sketch_dup_alloc {
 public:
  static const uint64_t MAX_THETA =
      LLONG_MAX;  // signed max for compatibility with Java
  static const uint8_t SERIAL_VERSION = 3;

  virtual ~theta_sketch_dup_alloc() = default;

  /**
   * @return true if this sketch represents an empty set (not the same as no
   * retained entries!)
   */
  bool is_empty() const;

  /**
   * @return estimate of the distinct count of the input stream
   */
  double get_estimate() const;

  /**
   * Returns the approximate lower error bound given a number of standard
   * deviations. This parameter is similar to the number of standard deviations
   * of the normal distribution and corresponds to approximately 67%, 95% and
   * 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the lower bound
   */
  double get_lower_bound(uint8_t num_std_devs) const;

  /**
   * Returns the approximate upper error bound given a number of standard
   * deviations. This parameter is similar to the number of standard deviations
   * of the normal distribution and corresponds to approximately 67%, 95% and
   * 99% confidence intervals.
   * @param num_std_devs number of Standard Deviations (1, 2 or 3)
   * @return the upper bound
   */
  double get_upper_bound(uint8_t num_std_devs) const;

  /**
   * @return true if the sketch is in estimation mode (as opposed to exact mode)
   */
  bool is_estimation_mode() const;

  /**
   * @return theta as a fraction from 0 to 1 (effective sampling rate)
   */
  double get_theta() const;

  /**
   * @return theta as a positive integer between 0 and LLONG_MAX
   */
  uint64_t get_theta64() const;

  /**
   * @return the number of retained entries in the sketch
   */
  virtual uint32_t get_num_retained() const = 0;

  virtual uint16_t get_seed_hash() const = 0;

  /**
   * @return true if retained entries are ordered
   */
  virtual bool is_ordered() const = 0;

  /**
   * Writes a human-readable summary of this sketch to a given stream
   * @param print_items if true include the list of items retained by the sketch
   */
  virtual string<A> to_string(bool print_items = false) const = 0;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  virtual void serialize(std::ostream& os) const = 0;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  typedef vector_u8<A> vector_bytes;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is an uninitialized space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   */
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const = 0;

  // This is a convenience alias for users
  // The type returned by the following deserialize methods
  // It is not possible to return instances of an abstract type, so this has to
  // be a pointer
  typedef std::unique_ptr<theta_sketch_dup_alloc<A>,
                          std::function<void(theta_sketch_dup_alloc<A>*)>>
      unique_ptr;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of a sketch as a unique_ptr
   */
  static unique_ptr deserialize(std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of the sketch
   */
  static unique_ptr deserialize(const void* bytes, size_t size,
                                uint64_t seed = DEFAULT_SEED);

  class const_iterator;

  /**
   * Iterator over hash values in this sketch.
   * @return begin iterator
   */
  virtual const_iterator begin() const = 0;

  /**
   * Iterator pointing past the valid range.
   * Not to be incremented or dereferenced.
   * @return end iterator
   */
  virtual const_iterator end() const = 0;

  /**
   * @return true if *this equals r
   */
  virtual bool isEqual(const theta_sketch_dup_alloc& r) const;

 protected:
  enum flags { IS_BIG_ENDIAN, IS_READ_ONLY, IS_EMPTY, IS_COMPACT, IS_ORDERED };

  bool is_empty_;
  uint64_t theta_;

  theta_sketch_dup_alloc(bool is_empty, uint64_t theta);

  static uint16_t get_seed_hash(uint64_t seed);

  static void check_sketch_type(uint8_t actual, uint8_t expected);
  static void check_serial_version(uint8_t actual, uint8_t expected);
  static void check_seed_hash(uint16_t actual, uint16_t expected);

  /* TODO: support set operations
   * friend theta_intersection_alloc<A>;
   * friend theta_a_not_b_alloc<A>;
   */
};

// update sketch

template <typename A>
using AllocU64 = typename std::allocator_traits<A>::template rebind_alloc<
    std::pair<uint64_t, uint64_t>>;
template <typename A>
using vector_u64 = std::vector<std::pair<uint64_t, uint64_t>, AllocU64<A>>;

template <typename A>
class update_theta_sketch_dup_alloc : public theta_sketch_dup_alloc<A> {
 public:
  class builder;
  enum resize_factor { X1, X2, X4, X8 };
  static const uint8_t SKETCH_TYPE = 2;

  // No constructor here. Use builder instead.

  virtual ~update_theta_sketch_dup_alloc() = default;

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual string<A> to_string(bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  typedef vector_u8<A> vector_bytes;  // alias for users
  // header space is reserved, but not initialized
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
   * Update this sketch with a given string.
   * @param value string to update the sketch with
   */
  void update(const std::string& value);

  /**
   * Update this sketch with a given unsigned 64-bit integer.
   * @param value uint64_t to update the sketch with
   */
  void update(uint64_t value);

  /**
   * Update this sketch with a given signed 64-bit integer.
   * @param value int64_t to update the sketch with
   */
  void update(int64_t value);

  /**
   * Update this sketch with a given unsigned 32-bit integer.
   * For compatibility with Java implementation.
   * @param value uint32_t to update the sketch with
   */
  void update(uint32_t value);

  /**
   * Update this sketch with a given signed 32-bit integer.
   * For compatibility with Java implementation.
   * @param value int32_t to update the sketch with
   */
  void update(int32_t value);

  /**
   * Update this sketch with a given unsigned 16-bit integer.
   * For compatibility with Java implementation.
   * @param value uint16_t to update the sketch with
   */
  void update(uint16_t value);

  /**
   * Update this sketch with a given signed 16-bit integer.
   * For compatibility with Java implementation.
   * @param value int16_t to update the sketch with
   */
  void update(int16_t value);

  /**
   * Update this sketch with a given unsigned 8-bit integer.
   * For compatibility with Java implementation.
   * @param value uint8_t to update the sketch with
   */
  void update(uint8_t value);

  /**
   * Update this sketch with a given signed 8-bit integer.
   * For compatibility with Java implementation.
   * @param value int8_t to update the sketch with
   */
  void update(int8_t value);

  /**
   * Update this sketch with a given double-precision floating point value.
   * For compatibility with Java implementation.
   * @param value double to update the sketch with
   */
  void update(double value);

  /**
   * Update this sketch with a given floating point value.
   * For compatibility with Java implementation.
   * @param value float to update the sketch with
   */
  void update(float value);

  /**
   * Update this sketch with given data of any type.
   * This is a "universal" update that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing sketches between C++ environment and Java environment.
   * Otherwise two sketches that should represent overlapping sets will be
   * disjoint For instance, for signed 32-bit values call update(int32_t) method
   * above, which does widening conversion to int64_t, if compatibility with
   * Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   */
  void update(const void* data, unsigned length);

  /**
   * Remove one string from the theta-sketch.
   * @param value string to be removed from the sketch
   */
  void remove(const std::string& value);

  /**
   * Remove one unsigned 64 bit integer from the theta-sketch.
   * @param value uint64_t to be removed from the sketch
   */
  void remove(uint64_t value);

  /**
   * Remove one signed 64 bit integer from the theta-sketch.
   * @param value int64_t to be removed from the sketch
   */
  void remove(int64_t value);

  /**
   * Remove one unsigned 32 bit integer from the theta-sketch.
   * @param value uint32_t to be removed from the sketch
   */
  void remove(uint32_t value);

  /**
   * Remove one signed 32 bit integer from the theta-sketch.
   * @param value int32_t to be removed from the sketch
   */
  void remove(int32_t value);

  /**
   * Remove one unsigned 16 bit integer from the theta-sketch.
   * @param value uint16_t to be removed from the sketch
   */
  void remove(uint16_t value);

  /**
   * Remove one signed 16 bit integer from the theta-sketch.
   * @param value int16_t to be removed from the sketch
   */
  void remove(int16_t value);

  /**
   * Remove one unsigned 8 bit integer from the theta-sketch.
   * @param value uint8_t to be removed from the sketch
   */
  void remove(uint8_t value);

  /**
   * Remove one signed 8 bit integer from the theta-sketch.
   * @param value int8_t to be removed from the sketch
   */
  void remove(int8_t value);

  /**
   * Remove one double-precision floating point value from the theta-sketch.
   * @param value double to be removed from the sketch
   */
  void remove(double value);

  /**
   * Remove one single-precision floating point value from the theta-sketch.
   * @param value float to be removed from the sketch
   */
  void remove(float value);

  /**
   * Remove one element of any type from this sketch.
   * This is a "universal" remove that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing sketches between C++ environment and Java environment.
   * Otherwise two sketches that should represent overlapping sets will be
   * disjoint For instance, for signed 32-bit values call update(int32_t) method
   * above, which does widening conversion to int64_t, if compatibility with
   * Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   */
  void remove(const void* data, unsigned length);

  /**
   * Remove retained entries in excess of the nominal size k (if any)
   */
  void trim();

  /**
   * Converts this sketch to a compact sketch (ordered or unordered).
   * @param ordered optional flag to specify if ordered sketch should be
   * produced
   * @return compact sketch
   */
  compact_theta_sketch_dup_alloc<A> compact(bool ordered = true) const;

  virtual typename theta_sketch_dup_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_dup_alloc<A>::const_iterator end() const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of a sketch
   */
  static update_theta_sketch_dup_alloc<A> deserialize(
      std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of the sketch
   */
  static update_theta_sketch_dup_alloc<A> deserialize(
      const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

  /**
   * @return true if *this equals r
   */
  virtual bool isEqual(const update_theta_sketch_dup_alloc& r) const;

 private:
  // resize threshold = 0.5 tuned for speed
  static constexpr double RESIZE_THRESHOLD = 0.5;
  // hash table rebuild threshold = 15/16
  static constexpr double REBUILD_THRESHOLD = 15.0 / 16.0;

  static constexpr uint8_t STRIDE_HASH_BITS = 7;
  static constexpr uint32_t STRIDE_MASK = (1 << STRIDE_HASH_BITS) - 1;

  uint8_t lg_cur_size_;
  uint8_t lg_nom_size_;

  /**
   * keys_ stores a vector of pairs: the first element in pairs represent the
   * hash value, the second element in pairs represent the number of times this
   * element shows up in the stream
   */
  vector_u64<A> keys_;
  uint32_t num_keys_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;
  uint32_t capacity_;

  // for builder
  update_theta_sketch_dup_alloc(uint8_t lg_cur_size, uint8_t lg_nom_size,
                                resize_factor rf, float p, uint64_t seed);

  // for deserialize
  update_theta_sketch_dup_alloc(bool is_empty, uint64_t theta,
                                uint8_t lg_cur_size, uint8_t lg_nom_size,
                                vector_u64<A>&& keys, uint32_t num_keys,
                                resize_factor rf, float p, uint64_t seed);

  void resize();
  void rebuild();

  // TODO: support union
  // friend theta_union_alloc<A>;
  void internal_update(uint64_t hash);
  void internal_remove(uint64_t hash);

  // TODO: support intersection
  // friend theta_intersection_alloc<A>;
  // TODO: support a_not_b
  // friend theta_a_not_b_alloc<A>;
  static inline uint32_t get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size);
  static inline uint32_t get_stride(uint64_t hash, uint8_t lg_size);

  /**
   * search hash values, if exists increase the count and return true, otherwise
   * insert the value and return false
   * @param hash: the hash value
   * @param table: the pointer to the hash table
   * @param table: lg_size of the current hash table
   */
  static bool hash_search_or_insert(uint64_t hash,
                                    std::pair<uint64_t, uint64_t>* table,
                                    uint8_t lg_size);
  /**
   * search hash values, if exists decrease the count
   * if the count==0, remove the element from hash table and return true else
   * return false
   * @param hash: the hash value
   * @param table: the pointer to the hash table
   * @param table: lg_size of the current hash table
   */
  static bool hash_search_or_remove(uint64_t hash,
                                    std::pair<uint64_t, uint64_t>* table,
                                    uint8_t lg_size);
  static bool hash_search(uint64_t hash,
                          const std::pair<uint64_t, uint64_t>* table,
                          uint8_t lg_size);

  friend theta_sketch_dup_alloc<A>;
  static update_theta_sketch_dup_alloc<A> internal_deserialize(
      std::istream& is, resize_factor rf, uint8_t lg_cur_size,
      uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed);
  static update_theta_sketch_dup_alloc<A> internal_deserialize(
      const void* bytes, size_t size, resize_factor rf, uint8_t lg_cur_size,
      uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed);
};

// compact sketch

template <typename A>
class compact_theta_sketch_dup_alloc : public theta_sketch_dup_alloc<A> {
 public:
  static const uint8_t SKETCH_TYPE = 3;

  // No constructor here.
  // Instances of this type can be obtained:
  // - by compacting an update_theta_sketch_dup
  // - as a result of a set operation
  // - by deserializing a previously serialized compact sketch

  compact_theta_sketch_dup_alloc(const theta_sketch_dup_alloc<A>& other,
                                 bool ordered);
  virtual ~compact_theta_sketch_dup_alloc() = default;

  virtual uint32_t get_num_retained() const;
  virtual uint16_t get_seed_hash() const;
  virtual bool is_ordered() const;
  virtual string<A> to_string(bool print_items = false) const;
  virtual void serialize(std::ostream& os) const;
  typedef vector_u8<A> vector_bytes;  // alias for users
  // header space is reserved, but not initialized
  virtual vector_bytes serialize(unsigned header_size_bytes = 0) const;

  virtual typename theta_sketch_dup_alloc<A>::const_iterator begin() const;
  virtual typename theta_sketch_dup_alloc<A>::const_iterator end() const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of a sketch
   */
  static compact_theta_sketch_dup_alloc<A> deserialize(
      std::istream& is, uint64_t seed = DEFAULT_SEED);

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param seed the seed for the hash function that was used to create the
   * sketch
   * @return an instance of the sketch
   */
  static compact_theta_sketch_dup_alloc<A> deserialize(
      const void* bytes, size_t size, uint64_t seed = DEFAULT_SEED);

  /**
   * @return true if *this equals r
   */
  virtual bool isEqual(const compact_theta_sketch_dup_alloc& r) const;

 private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t>
      AllocU64;

  vector_u64<A> keys_;
  uint16_t seed_hash_;
  bool is_ordered_;

  friend theta_sketch_dup_alloc<A>;
  friend update_theta_sketch_dup_alloc<A>;

  /* TODO: support set operations
   * friend theta_union_alloc<A>;
   * friend theta_intersection_alloc<A>;
   * friend theta_a_not_b_alloc<A>;
   */

  compact_theta_sketch_dup_alloc(bool is_empty, uint64_t theta,
                                 vector_u64<A>&& keys, uint16_t seed_hash,
                                 bool is_ordered);
  static compact_theta_sketch_dup_alloc<A> internal_deserialize(
      std::istream& is, uint8_t preamble_longs, uint8_t flags_byte,
      uint16_t seed_hash);
  static compact_theta_sketch_dup_alloc<A> internal_deserialize(
      const void* bytes, size_t size, uint8_t preamble_longs,
      uint8_t flags_byte, uint16_t seed_hash);
};

// builder

template <typename A>
class update_theta_sketch_dup_alloc<A>::builder {
 public:
  static const uint8_t MIN_LG_K = 5;
  static const uint8_t DEFAULT_LG_K = 12;
  static const resize_factor DEFAULT_RESIZE_FACTOR = X8;

  /**
   * Creates and instance of the builder with default parameters.
   */
  builder();

  /**
   * Set log2(k), where k is a nominal number of entries in the sketch
   * @param lg_k base 2 logarithm of nominal number of entries
   * @return this builder
   */
  builder& set_lg_k(uint8_t lg_k);

  /**
   * Set resize factor for the internal hash table (defaults to 8)
   * @param rf resize factor
   * @return this builder
   */
  builder& set_resize_factor(resize_factor rf);

  /**
   * Set sampling probability (initial theta). The default is 1, so the sketch
   * retains all entries until it reaches the limit, at which point it goes into
   * the estimation mode and reduces the effective sampling probability (theta)
   * as necessary.
   * @param p sampling probability
   * @return this builder
   */
  builder& set_p(float p);

  /**
   * Set the seed for the hash function. Should be used carefully if needed.
   * Sketches produced with different seed are not compatible
   * and cannot be mixed in set operations.
   * @param seed hash seed
   * @return this builder
   */
  builder& set_seed(uint64_t seed);

  /**
   * This is to create an instance of the sketch with predefined parameters.
   * @return and instance of the sketch
   */
  update_theta_sketch_dup_alloc<A> build() const;

 private:
  uint8_t lg_k_;
  resize_factor rf_;
  float p_;
  uint64_t seed_;

  static uint8_t starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min,
                                       uint8_t lg_rf);
};

// iterator
template <typename A>
class theta_sketch_dup_alloc<A>::const_iterator
    : public std::iterator<std::input_iterator_tag,
                           std::pair<uint64_t, uint64_t>> {
 public:
  const_iterator& operator++();
  const_iterator operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  std::pair<uint64_t, uint64_t> operator*() const;

 private:
  const std::pair<uint64_t, uint64_t>* keys_;
  uint32_t size_;
  uint32_t index_;
  const_iterator(const std::pair<uint64_t, uint64_t>* keys, uint32_t size,
                 uint32_t index);
  friend class update_theta_sketch_dup_alloc<A>;
  friend class compact_theta_sketch_dup_alloc<A>;
};

/*
 * The following are implementations
 */
template <typename A>
theta_sketch_dup_alloc<A>::theta_sketch_dup_alloc(bool is_empty, uint64_t theta)
    : is_empty_(is_empty), theta_(theta) {}

template <typename A>
bool theta_sketch_dup_alloc<A>::is_empty() const {
  return is_empty_;
}

template <typename A>
double theta_sketch_dup_alloc<A>::get_estimate() const {
  return get_num_retained() / get_theta();
}

template <typename A>
double theta_sketch_dup_alloc<A>::get_lower_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_lower_bound(get_num_retained(), get_theta(),
                                          num_std_devs);
}

template <typename A>
double theta_sketch_dup_alloc<A>::get_upper_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_upper_bound(get_num_retained(), get_theta(),
                                          num_std_devs);
}

template <typename A>
bool theta_sketch_dup_alloc<A>::is_estimation_mode() const {
  return theta_ < MAX_THETA && !is_empty_;
}

template <typename A>
double theta_sketch_dup_alloc<A>::get_theta() const {
  return (double)theta_ / MAX_THETA;
}

template <typename A>
uint64_t theta_sketch_dup_alloc<A>::get_theta64() const {
  return theta_;
}

template <typename A>
typename theta_sketch_dup_alloc<A>::unique_ptr
theta_sketch_dup_alloc<A>::deserialize(std::istream& is, uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint8_t lg_nom_size;
  is.read((char*)&lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  is.read((char*)&lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));

  check_serial_version(serial_version, SERIAL_VERSION);
  check_seed_hash(seed_hash, get_seed_hash(seed));

  if (type == update_theta_sketch_dup_alloc<A>::SKETCH_TYPE) {
    typename update_theta_sketch_dup_alloc<A>::resize_factor rf =
        static_cast<typename update_theta_sketch_dup_alloc<A>::resize_factor>(
            preamble_longs >> 6);
    typedef typename std::allocator_traits<A>::template rebind_alloc<
        update_theta_sketch_dup_alloc<A>>
        AU;
    return unique_ptr(
        static_cast<theta_sketch_dup_alloc<A>*>(
            new (AU().allocate(1)) update_theta_sketch_dup_alloc<A>(
                update_theta_sketch_dup_alloc<A>::internal_deserialize(
                    is, rf, lg_cur_size, lg_nom_size, flags_byte, seed))),
        [](theta_sketch_dup_alloc<A>* ptr) {
          ptr->~theta_sketch_dup_alloc();
          AU().deallocate(static_cast<update_theta_sketch_dup_alloc<A>*>(ptr),
                          1);
        });
  } else if (type == compact_theta_sketch_dup_alloc<A>::SKETCH_TYPE) {
    typedef typename std::allocator_traits<A>::template rebind_alloc<
        compact_theta_sketch_dup_alloc<A>>
        AC;
    return unique_ptr(
        static_cast<theta_sketch_dup_alloc<A>*>(
            new (AC().allocate(1)) compact_theta_sketch_dup_alloc<A>(
                compact_theta_sketch_dup_alloc<A>::internal_deserialize(
                    is, preamble_longs, flags_byte, seed_hash))),
        [](theta_sketch_dup_alloc<A>* ptr) {
          ptr->~theta_sketch_dup_alloc();
          AC().deallocate(static_cast<compact_theta_sketch_dup_alloc<A>*>(ptr),
                          1);
        });
  }
  throw std::invalid_argument("unsupported sketch type " +
                              std::to_string((int)type));
}

template <typename A>
typename theta_sketch_dup_alloc<A>::unique_ptr
theta_sketch_dup_alloc<A>::deserialize(const void* bytes, size_t size,
                                       uint64_t seed) {
  ensure_minimum_memory(size, static_cast<size_t>(8));
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, &preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  ptr += copy_from_mem(ptr, &type, sizeof(type));
  uint8_t lg_nom_size;
  ptr += copy_from_mem(ptr, &lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  ptr += copy_from_mem(ptr, &lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, &seed_hash, sizeof(seed_hash));

  check_serial_version(serial_version, SERIAL_VERSION);
  check_seed_hash(seed_hash, get_seed_hash(seed));

  if (type == update_theta_sketch_dup_alloc<A>::SKETCH_TYPE) {
    typename update_theta_sketch_dup_alloc<A>::resize_factor rf =
        static_cast<typename update_theta_sketch_dup_alloc<A>::resize_factor>(
            preamble_longs >> 6);
    typedef typename std::allocator_traits<A>::template rebind_alloc<
        update_theta_sketch_dup_alloc<A>>
        AU;
    return unique_ptr(
        static_cast<theta_sketch_dup_alloc<A>*>(
            new (AU().allocate(1)) update_theta_sketch_dup_alloc<A>(
                update_theta_sketch_dup_alloc<A>::internal_deserialize(
                    ptr, size - (ptr - static_cast<const char*>(bytes)), rf,
                    lg_cur_size, lg_nom_size, flags_byte, seed))),
        [](theta_sketch_dup_alloc<A>* ptr) {
          ptr->~theta_sketch_dup_alloc();
          AU().deallocate(static_cast<update_theta_sketch_dup_alloc<A>*>(ptr),
                          1);
        });
  } else if (type == compact_theta_sketch_dup_alloc<A>::SKETCH_TYPE) {
    typedef typename std::allocator_traits<A>::template rebind_alloc<
        compact_theta_sketch_dup_alloc<A>>
        AC;
    return unique_ptr(
        static_cast<theta_sketch_dup_alloc<A>*>(
            new (AC().allocate(1)) compact_theta_sketch_dup_alloc<A>(
                compact_theta_sketch_dup_alloc<A>::internal_deserialize(
                    ptr, size - (ptr - static_cast<const char*>(bytes)),
                    preamble_longs, flags_byte, seed_hash))),
        [](theta_sketch_dup_alloc<A>* ptr) {
          ptr->~theta_sketch_dup_alloc();
          AC().deallocate(static_cast<compact_theta_sketch_dup_alloc<A>*>(ptr),
                          1);
        });
  }
  throw std::invalid_argument("unsupported sketch type " +
                              std::to_string((int)type));
}

template <typename A>
bool theta_sketch_dup_alloc<A>::isEqual(const theta_sketch_dup_alloc<A>& r) const {
  return this->theta_==r.theta_;
}

template <typename A>
uint16_t theta_sketch_dup_alloc<A>::get_seed_hash(uint64_t seed) {
  HashState hashes;
  MurmurHash3_x64_128(&seed, sizeof(seed), 0, hashes);
  return hashes.h1;
}

template <typename A>
void theta_sketch_dup_alloc<A>::check_sketch_type(uint8_t actual,
                                                  uint8_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch type mismatch: expected " +
                                std::to_string((int)expected) + ", actual " +
                                std::to_string((int)actual));
  }
}

template <typename A>
void theta_sketch_dup_alloc<A>::check_serial_version(uint8_t actual,
                                                     uint8_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch serial version mismatch: expected " +
                                std::to_string((int)expected) + ", actual " +
                                std::to_string((int)actual));
  }
}

template <typename A>
void theta_sketch_dup_alloc<A>::check_seed_hash(uint16_t actual,
                                                uint16_t expected) {
  if (actual != expected) {
    throw std::invalid_argument("Sketch seed hash mismatch: expected " +
                                std::to_string(expected) + ", actual " +
                                std::to_string(actual));
  }
}

// update sketch

template <typename A>
update_theta_sketch_dup_alloc<A>::update_theta_sketch_dup_alloc(
    uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p,
    uint64_t seed)
    : theta_sketch_dup_alloc<A>(true, theta_sketch_dup_alloc<A>::MAX_THETA),
      lg_cur_size_(lg_cur_size),
      lg_nom_size_(lg_nom_size),
      keys_(1 << lg_cur_size_, std::make_pair(0, 0)),
      num_keys_(0),
      rf_(rf),
      p_(p),
      seed_(seed),
      capacity_(get_capacity(lg_cur_size, lg_nom_size)) {
  if (p < 1) this->theta_ *= p;
}

template <typename A>
update_theta_sketch_dup_alloc<A>::update_theta_sketch_dup_alloc(
    bool is_empty, uint64_t theta, uint8_t lg_cur_size, uint8_t lg_nom_size,
    vector_u64<A>&& keys, uint32_t num_keys, resize_factor rf, float p,
    uint64_t seed)
    : theta_sketch_dup_alloc<A>(is_empty, theta),
      lg_cur_size_(lg_cur_size),
      lg_nom_size_(lg_nom_size),
      keys_(std::move(keys)),
      num_keys_(num_keys),
      rf_(rf),
      p_(p),
      seed_(seed),
      capacity_(get_capacity(lg_cur_size, lg_nom_size)) {}

template <typename A>
uint32_t update_theta_sketch_dup_alloc<A>::get_num_retained() const {
  return num_keys_;
}

template <typename A>
uint16_t update_theta_sketch_dup_alloc<A>::get_seed_hash() const {
  return theta_sketch_dup_alloc<A>::get_seed_hash(seed_);
}

template <typename A>
bool update_theta_sketch_dup_alloc<A>::is_ordered() const {
  return false;
}

template <typename A>
string<A> update_theta_sketch_dup_alloc<A>::to_string(bool print_items) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  os << "### Update Theta sketch summary:" << std::endl;
  os << "   lg nominal size      : " << (int)lg_nom_size_ << std::endl;
  os << "   lg current size      : " << (int)lg_cur_size_ << std::endl;
  os << "   num retained keys    : " << num_keys_ << std::endl;
  os << "   resize factor        : " << (1 << rf_) << std::endl;
  os << "   sampling probability : " << p_ << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   empty?               : " << (this->is_empty() ? "true" : "false")
     << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false")
     << std::endl;
  os << "   estimation mode?     : "
     << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->theta_ << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  os << "### End sketch summary" << std::endl;
  if (print_items) {
    os << "### Retained keys" << std::endl;
    for (auto key : *this) os << "   " << key << std::endl;
    os << "### End retained keys" << std::endl;
  }
  return os.str();
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs_and_rf = 3 | (rf_ << 6);
  os.write((char*)&preamble_longs_and_rf, sizeof(preamble_longs_and_rf));
  const uint8_t serial_version = theta_sketch_dup_alloc<A>::SERIAL_VERSION;
  os.write((char*)&serial_version, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  os.write((char*)&type, sizeof(type));
  os.write((char*)&lg_nom_size_, sizeof(lg_nom_size_));
  os.write((char*)&lg_cur_size_, sizeof(lg_cur_size_));
  const uint8_t flags_byte(
      (this->is_empty() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY : 0));
  os.write((char*)&flags_byte, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  os.write((char*)&seed_hash, sizeof(seed_hash));
  os.write((char*)&num_keys_, sizeof(num_keys_));
  os.write((char*)&p_, sizeof(p_));
  os.write((char*)&(this->theta_), sizeof(uint64_t));
  // updated in dup: pair instead of uint64_t
  os.write((char*)keys_.data(),
           sizeof(std::pair<uint64_t, uint64_t>) * keys_.size());
}

template <typename A>
vector_u8<A> update_theta_sketch_dup_alloc<A>::serialize(
    unsigned header_size_bytes) const {
  const uint8_t preamble_longs = 3;
  // updated in dup: pair instead of uint64_t
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs +
                      sizeof(std::pair<uint64_t, uint64_t>) * keys_.size();
  vector_u8<A> bytes(size);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  const uint8_t preamble_longs_and_rf = preamble_longs | (rf_ << 6);
  ptr +=
      copy_to_mem(&preamble_longs_and_rf, ptr, sizeof(preamble_longs_and_rf));
  const uint8_t serial_version = theta_sketch_dup_alloc<A>::SERIAL_VERSION;
  ptr += copy_to_mem(&serial_version, ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  ptr += copy_to_mem(&type, ptr, sizeof(type));
  ptr += copy_to_mem(&lg_nom_size_, ptr, sizeof(lg_nom_size_));
  ptr += copy_to_mem(&lg_cur_size_, ptr, sizeof(lg_cur_size_));
  const uint8_t flags_byte(
      (this->is_empty() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY : 0));
  ptr += copy_to_mem(&flags_byte, ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  ptr += copy_to_mem(&seed_hash, ptr, sizeof(seed_hash));
  ptr += copy_to_mem(&num_keys_, ptr, sizeof(num_keys_));
  ptr += copy_to_mem(&p_, ptr, sizeof(p_));
  ptr += copy_to_mem(&(this->theta_), ptr, sizeof(uint64_t));
  ptr += copy_to_mem(keys_.data(), ptr,
                     sizeof(std::pair<uint64_t, uint64_t>) * keys_.size());

  return bytes;
}

template <typename A>
update_theta_sketch_dup_alloc<A> update_theta_sketch_dup_alloc<A>::deserialize(
    std::istream& is, uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  resize_factor rf = static_cast<resize_factor>(preamble_longs >> 6);
  preamble_longs &= 0x3f;  // remove resize factor
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint8_t lg_nom_size;
  is.read((char*)&lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  is.read((char*)&lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));
  theta_sketch_dup_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_dup_alloc<A>::check_serial_version(
      serial_version, theta_sketch_dup_alloc<A>::SERIAL_VERSION);
  theta_sketch_dup_alloc<A>::check_seed_hash(
      seed_hash, theta_sketch_dup_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(is, rf, lg_cur_size, lg_nom_size, flags_byte,
                              seed);
}

template <typename A>
update_theta_sketch_dup_alloc<A>
update_theta_sketch_dup_alloc<A>::internal_deserialize(
    std::istream& is, resize_factor rf, uint8_t lg_cur_size,
    uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed) {
  uint32_t num_keys;
  is.read((char*)&num_keys, sizeof(num_keys));
  float p;
  is.read((char*)&p, sizeof(p));
  uint64_t theta;
  is.read((char*)&theta, sizeof(theta));
  vector_u64<A> keys(1 << lg_cur_size);
  is.read((char*)keys.data(),
          sizeof(std::pair<uint64_t, uint64_t>) * keys.size());
  const bool is_empty =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY);
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return update_theta_sketch_dup_alloc<A>(is_empty, theta, lg_cur_size,
                                          lg_nom_size, std::move(keys),
                                          num_keys, rf, p, seed);
}

template <typename A>
update_theta_sketch_dup_alloc<A> update_theta_sketch_dup_alloc<A>::deserialize(
    const void* bytes, size_t size, uint64_t seed) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, &preamble_longs, sizeof(preamble_longs));
  resize_factor rf = static_cast<resize_factor>(preamble_longs >> 6);
  preamble_longs &= 0x3f;  // remove resize factor
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  ptr += copy_from_mem(ptr, &type, sizeof(type));
  uint8_t lg_nom_size;
  ptr += copy_from_mem(ptr, &lg_nom_size, sizeof(lg_nom_size));
  uint8_t lg_cur_size;
  ptr += copy_from_mem(ptr, &lg_cur_size, sizeof(lg_cur_size));
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, &seed_hash, sizeof(seed_hash));
  theta_sketch_dup_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_dup_alloc<A>::check_serial_version(
      serial_version, theta_sketch_dup_alloc<A>::SERIAL_VERSION);
  theta_sketch_dup_alloc<A>::check_seed_hash(
      seed_hash, theta_sketch_dup_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(ptr,
                              size - (ptr - static_cast<const char*>(bytes)),
                              rf, lg_cur_size, lg_nom_size, flags_byte, seed);
}

template <typename A>
bool update_theta_sketch_dup_alloc<A>::isEqual(const update_theta_sketch_dup_alloc<A>& r) const {
  return this->theta_==r.theta_;
}

template <typename A>
update_theta_sketch_dup_alloc<A>
update_theta_sketch_dup_alloc<A>::internal_deserialize(
    const void* bytes, size_t size, resize_factor rf, uint8_t lg_cur_size,
    uint8_t lg_nom_size, uint8_t flags_byte, uint64_t seed) {
  const uint32_t table_size = 1 << lg_cur_size;
  ensure_minimum_memory(size, 16 + sizeof(uint64_t) * table_size);
  const char* ptr = static_cast<const char*>(bytes);
  uint32_t num_keys;
  ptr += copy_from_mem(ptr, &num_keys, sizeof(num_keys));
  float p;
  ptr += copy_from_mem(ptr, &p, sizeof(p));
  uint64_t theta;
  ptr += copy_from_mem(ptr, &theta, sizeof(theta));
  vector_u64<A> keys(table_size);
  ptr += copy_from_mem(ptr, keys.data(),
                       sizeof(std::pair<uint64_t, uint64_t>) * table_size);
  const bool is_empty =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY);
  return update_theta_sketch_dup_alloc<A>(is_empty, theta, lg_cur_size,
                                          lg_nom_size, std::move(keys),
                                          num_keys, rf, p, seed);
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(const std::string& value) {
  if (value.empty()) return;
  update(value.c_str(), value.length());
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(uint64_t value) {
  update(&value, sizeof(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(int64_t value) {
  update(&value, sizeof(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(uint32_t value) {
  update(static_cast<int32_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(int32_t value) {
  update(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(uint16_t value) {
  update(static_cast<int16_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(int16_t value) {
  update(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(uint8_t value) {
  update(static_cast<int8_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(int8_t value) {
  update(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(double value) {
  union {
    int64_t long_value;
    double double_value;
  } long_double_union;

  if (value == 0.0) {
    long_double_union.double_value = 0.0;  // canonicalize -0.0 to 0.0
  } else if (std::isnan(value)) {
    long_double_union.long_value =
        0x7ff8000000000000L;  // canonicalize NaN using value from Java's
                              // Double.doubleToLongBits()
  } else {
    long_double_union.double_value = value;
  }
  update(&long_double_union, sizeof(long_double_union));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(float value) {
  update(static_cast<double>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::update(const void* data,
                                              unsigned length) {
  HashState hashes;
  MurmurHash3_x64_128(data, length, seed_, hashes);
  const uint64_t hash =
      hashes.h1 >>
      1;  // Java implementation does logical shift >>> to make values positive
  internal_update(hash);
}

template <typename A>
compact_theta_sketch_dup_alloc<A> update_theta_sketch_dup_alloc<A>::compact(
    bool ordered) const {
  return compact_theta_sketch_dup_alloc<A>(*this, ordered);
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::internal_update(uint64_t hash) {
  this->is_empty_ = false;
  if (hash >= this->theta_ || hash == 0)
    return;  // hash == 0 is reserved to mark empty slots in the table
  if (hash_search_or_insert(hash, keys_.data(), lg_cur_size_)) {
    num_keys_++;
    if (num_keys_ > capacity_) {
      if (lg_cur_size_ <= lg_nom_size_) {
        resize();
      } else {
        rebuild();
      }
    }
  }
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::trim() {
  if (num_keys_ > static_cast<uint32_t>(1 << lg_nom_size_)) rebuild();
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::resize() {
  const uint8_t lg_tgt_size = lg_nom_size_ + 1;
  const uint8_t factor =
      std::max(1, std::min(static_cast<int>(rf_), lg_tgt_size - lg_cur_size_));
  const uint8_t lg_new_size = lg_cur_size_ + factor;
  const uint32_t new_size = 1 << lg_new_size;
  vector_u64<A> new_keys(new_size, std::make_pair(0, 0));
  for (uint32_t i = 0; i < keys_.size(); i++) {
    if (keys_[i].first != 0) {
      hash_search_or_insert(keys_[i].first, new_keys.data(),
                            lg_new_size);  // TODO hash_insert
    }
  }
  keys_ = std::move(new_keys);
  lg_cur_size_ += factor;
  capacity_ = get_capacity(lg_cur_size_, lg_nom_size_);
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::rebuild() {
  const uint32_t pivot = (1 << lg_nom_size_) + keys_.size() - num_keys_;
  std::nth_element(&keys_[0], &keys_[pivot], &keys_[keys_.size()]);
  this->theta_ = keys_[pivot].first;
  vector_u64<A> new_keys(keys_.size(), std::make_pair(0, 0));
  num_keys_ = 0;
  for (uint32_t i = 0; i < keys_.size(); i++) {
    if (keys_[i].first != 0 && keys_[i].first < this->theta_) {
      hash_search_or_insert(keys_[i].first, new_keys.data(),
                            lg_cur_size_);  // TODO hash_insert
      num_keys_++;
    }
  }
  keys_ = std::move(new_keys);
}

template <typename A>
uint32_t update_theta_sketch_dup_alloc<A>::get_capacity(uint8_t lg_cur_size,
                                                        uint8_t lg_nom_size) {
  const double fraction =
      (lg_cur_size <= lg_nom_size) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
  return std::floor(fraction * (1 << lg_cur_size));
}

template <typename A>
uint32_t update_theta_sketch_dup_alloc<A>::get_stride(uint64_t hash,
                                                      uint8_t lg_size) {
  // odd and independent of index assuming lg_size lowest bits of the hash were
  // used for the index
  return (2 * static_cast<uint32_t>((hash >> lg_size) & STRIDE_MASK)) + 1;
}

template <typename A>
bool update_theta_sketch_dup_alloc<A>::hash_search_or_insert(
    uint64_t hash, std::pair<uint64_t, uint64_t>* table, uint8_t lg_size) {
  const uint32_t mask = (1 << lg_size) - 1;
  const uint32_t stride = get_stride(hash, lg_size);
  uint32_t cur_probe = static_cast<uint32_t>(hash) & mask;

  // search for duplicate or zero
  const uint32_t loop_index = cur_probe;
  do {
    const uint64_t value = table[cur_probe].first;
    if (value == 0) {
      table[cur_probe].first = hash;  // insert value
      table[cur_probe].second = 1;    // set the initial count to be 1
      return true;
    } else if (value == hash) {
      table[cur_probe].second++;  // count++
      return false;               // found a duplicate
    }
    cur_probe = (cur_probe + stride) & mask;
  } while (cur_probe != loop_index);
  throw std::logic_error("key not found and no empty slots!");
}

template <typename A>
bool update_theta_sketch_dup_alloc<A>::hash_search(
    uint64_t hash, const std::pair<uint64_t, uint64_t>* table,
    uint8_t lg_size) {
  const uint32_t mask = (1 << lg_size) - 1;
  const uint32_t stride =
      update_theta_sketch_dup_alloc<A>::get_stride(hash, lg_size);
  uint32_t cur_probe = static_cast<uint32_t>(hash) & mask;
  const uint32_t loop_index = cur_probe;
  do {
    const uint64_t value = table[cur_probe].first;
    if (value == 0) {
      return false;
    } else if (value == hash) {
      return true;
    }
    cur_probe = (cur_probe + stride) & mask;
  } while (cur_probe != loop_index);
  throw std::logic_error("key not found and search wrapped");
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator
update_theta_sketch_dup_alloc<A>::begin() const {
  return typename theta_sketch_dup_alloc<A>::const_iterator(keys_.data(),
                                                            keys_.size(), 0);
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator
update_theta_sketch_dup_alloc<A>::end() const {
  return typename theta_sketch_dup_alloc<A>::const_iterator(
      keys_.data(), keys_.size(), keys_.size());
}

// compact sketch

template <typename A>
compact_theta_sketch_dup_alloc<A>::compact_theta_sketch_dup_alloc(
    bool is_empty, uint64_t theta, vector_u64<A>&& keys, uint16_t seed_hash,
    bool is_ordered)
    : theta_sketch_dup_alloc<A>(is_empty, theta),
      keys_(std::move(keys)),
      seed_hash_(seed_hash),
      is_ordered_(is_ordered) {}

template <typename A>
compact_theta_sketch_dup_alloc<A>::compact_theta_sketch_dup_alloc(
    const theta_sketch_dup_alloc<A>& other, bool ordered)
    : theta_sketch_dup_alloc<A>(other),
      keys_(other.get_num_retained()),
      seed_hash_(other.get_seed_hash()),
      is_ordered_(other.is_ordered() || ordered) {
  std::copy(other.begin(), other.end(), keys_.begin());
  if (ordered && !other.is_ordered()) std::sort(keys_.begin(), keys_.end());
}

template <typename A>
uint32_t compact_theta_sketch_dup_alloc<A>::get_num_retained() const {
  return keys_.size();
}

template <typename A>
uint16_t compact_theta_sketch_dup_alloc<A>::get_seed_hash() const {
  return seed_hash_;
}

template <typename A>
bool compact_theta_sketch_dup_alloc<A>::is_ordered() const {
  return is_ordered_;
}

template <typename A>
string<A> compact_theta_sketch_dup_alloc<A>::to_string(bool print_items) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  os << "### Compact Theta sketch summary:" << std::endl;
  os << "   num retained keys    : " << keys_.size() << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   empty?               : " << (this->is_empty() ? "true" : "false")
     << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false")
     << std::endl;
  os << "   estimation mode?     : "
     << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->theta_ << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  os << "### End sketch summary" << std::endl;
  if (print_items) {
    os << "### Retained keys" << std::endl;
    for (auto key : *this) os << "   " << key << std::endl;
    os << "### End retained keys" << std::endl;
  }
  return os.str();
}

template <typename A>
void compact_theta_sketch_dup_alloc<A>::serialize(std::ostream& os) const {
  const bool is_single_item = keys_.size() == 1 && !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() || is_single_item
                                     ? 1
                                     : this->is_estimation_mode() ? 3 : 2;
  os.write((char*)&preamble_longs, sizeof(preamble_longs));
  const uint8_t serial_version = theta_sketch_dup_alloc<A>::SERIAL_VERSION;
  os.write((char*)&serial_version, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  os.write((char*)&type, sizeof(type));
  const uint16_t unused16 = 0;
  os.write((char*)&unused16, sizeof(unused16));
  const uint8_t flags_byte(
      (1 << theta_sketch_dup_alloc<A>::flags::IS_COMPACT) |
      (1 << theta_sketch_dup_alloc<A>::flags::IS_READ_ONLY) |
      (this->is_empty() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY : 0) |
      (this->is_ordered() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_ORDERED
                          : 0));
  os.write((char*)&flags_byte, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  os.write((char*)&seed_hash, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      const uint32_t num_keys = keys_.size();
      os.write((char*)&num_keys, sizeof(num_keys));
      const uint32_t unused32 = 0;
      os.write((char*)&unused32, sizeof(unused32));
      if (this->is_estimation_mode()) {
        os.write((char*)&(this->theta_), sizeof(uint64_t));
      }
    }
    os.write((char*)keys_.data(),
             sizeof(std::pair<uint64_t, uint64_t>) * keys_.size());
  }
}

template <typename A>
vector_u8<A> compact_theta_sketch_dup_alloc<A>::serialize(
    unsigned header_size_bytes) const {
  const bool is_single_item = keys_.size() == 1 && !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() || is_single_item
                                     ? 1
                                     : this->is_estimation_mode() ? 3 : 2;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs +
                      sizeof(std::pair<uint64_t, uint64_t>) * keys_.size();
  vector_u8<A> bytes(size);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  ptr += copy_to_mem(&preamble_longs, ptr, sizeof(preamble_longs));
  const uint8_t serial_version = theta_sketch_dup_alloc<A>::SERIAL_VERSION;
  ptr += copy_to_mem(&serial_version, ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  ptr += copy_to_mem(&type, ptr, sizeof(type));
  const uint16_t unused16 = 0;
  ptr += copy_to_mem(&unused16, ptr, sizeof(unused16));
  const uint8_t flags_byte(
      (1 << theta_sketch_dup_alloc<A>::flags::IS_COMPACT) |
      (1 << theta_sketch_dup_alloc<A>::flags::IS_READ_ONLY) |
      (this->is_empty() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY : 0) |
      (this->is_ordered() ? 1 << theta_sketch_dup_alloc<A>::flags::IS_ORDERED
                          : 0));
  ptr += copy_to_mem(&flags_byte, ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  ptr += copy_to_mem(&seed_hash, ptr, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      const uint32_t num_keys = keys_.size();
      ptr += copy_to_mem(&num_keys, ptr, sizeof(num_keys));
      const uint32_t unused32 = 0;
      ptr += copy_to_mem(&unused32, ptr, sizeof(unused32));
      if (this->is_estimation_mode()) {
        ptr += copy_to_mem(&(this->theta_), ptr, sizeof(uint64_t));
      }
    }
    ptr += copy_to_mem(keys_.data(), ptr,
                       sizeof(std::pair<uint64_t, uint64_t>) * keys_.size());
  }

  return bytes;
}

template <typename A>
compact_theta_sketch_dup_alloc<A>
compact_theta_sketch_dup_alloc<A>::deserialize(std::istream& is,
                                               uint64_t seed) {
  uint8_t preamble_longs;
  is.read((char*)&preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  is.read((char*)&serial_version, sizeof(serial_version));
  uint8_t type;
  is.read((char*)&type, sizeof(type));
  uint16_t unused16;
  is.read((char*)&unused16, sizeof(unused16));
  uint8_t flags_byte;
  is.read((char*)&flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  is.read((char*)&seed_hash, sizeof(seed_hash));
  theta_sketch_dup_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_dup_alloc<A>::check_serial_version(
      serial_version, theta_sketch_dup_alloc<A>::SERIAL_VERSION);
  theta_sketch_dup_alloc<A>::check_seed_hash(
      seed_hash, theta_sketch_dup_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(is, preamble_longs, flags_byte, seed_hash);
}

template <typename A>
bool compact_theta_sketch_dup_alloc<A>::isEqual(const compact_theta_sketch_dup_alloc<A>& r) const {
  return this->theta_==r.theta_;
}

template <typename A>
compact_theta_sketch_dup_alloc<A>
compact_theta_sketch_dup_alloc<A>::internal_deserialize(std::istream& is,
                                                        uint8_t preamble_longs,
                                                        uint8_t flags_byte,
                                                        uint16_t seed_hash) {
  uint64_t theta = theta_sketch_dup_alloc<A>::MAX_THETA;
  uint32_t num_keys = 0;

  const bool is_empty =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY);
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_keys = 1;
    } else {
      is.read((char*)&num_keys, sizeof(num_keys));
      uint32_t unused32;
      is.read((char*)&unused32, sizeof(unused32));
      if (preamble_longs > 2) {
        is.read((char*)&theta, sizeof(theta));
      }
    }
  }
  vector_u64<A> keys(num_keys);
  if (!is_empty)
    is.read((char*)keys.data(),
            sizeof(std::pair<uint64_t, uint64_t>) * keys.size());

  const bool is_ordered =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_ORDERED);
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return compact_theta_sketch_dup_alloc<A>(is_empty, theta, std::move(keys),
                                           seed_hash, is_ordered);
}

template <typename A>
compact_theta_sketch_dup_alloc<A>
compact_theta_sketch_dup_alloc<A>::deserialize(const void* bytes, size_t size,
                                               uint64_t seed) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  uint8_t preamble_longs;
  ptr += copy_from_mem(ptr, &preamble_longs, sizeof(preamble_longs));
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, &serial_version, sizeof(serial_version));
  uint8_t type;
  ptr += copy_from_mem(ptr, &type, sizeof(type));
  uint16_t unused16;
  ptr += copy_from_mem(ptr, &unused16, sizeof(unused16));
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, &flags_byte, sizeof(flags_byte));
  uint16_t seed_hash;
  ptr += copy_from_mem(ptr, &seed_hash, sizeof(seed_hash));
  theta_sketch_dup_alloc<A>::check_sketch_type(type, SKETCH_TYPE);
  theta_sketch_dup_alloc<A>::check_serial_version(
      serial_version, theta_sketch_dup_alloc<A>::SERIAL_VERSION);
  theta_sketch_dup_alloc<A>::check_seed_hash(
      seed_hash, theta_sketch_dup_alloc<A>::get_seed_hash(seed));
  return internal_deserialize(ptr,
                              size - (ptr - static_cast<const char*>(bytes)),
                              preamble_longs, flags_byte, seed_hash);
}

template <typename A>
compact_theta_sketch_dup_alloc<A>
compact_theta_sketch_dup_alloc<A>::internal_deserialize(const void* bytes,
                                                        size_t size,
                                                        uint8_t preamble_longs,
                                                        uint8_t flags_byte,
                                                        uint16_t seed_hash) {
  const char* ptr = static_cast<const char*>(bytes);
  const char* base = ptr;

  uint64_t theta = theta_sketch_dup_alloc<A>::MAX_THETA;
  uint32_t num_keys = 0;

  const bool is_empty =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_EMPTY);
  if (!is_empty) {
    if (preamble_longs == 1) {
      num_keys = 1;
    } else {
      ensure_minimum_memory(size,
                            8);  // read the first prelong before this method
      ptr += copy_from_mem(ptr, &num_keys, sizeof(num_keys));
      uint32_t unused32;
      ptr += copy_from_mem(ptr, &unused32, sizeof(unused32));
      if (preamble_longs > 2) {
        ensure_minimum_memory(size, (preamble_longs - 1) << 3);
        ptr += copy_from_mem(ptr, &theta, sizeof(theta));
      }
    }
  }
  const size_t keys_size_bytes =
      sizeof(std::pair<uint64_t, uint64_t>) * num_keys;
  check_memory_size(ptr - base + keys_size_bytes, size);
  vector_u64<A> keys(num_keys);
  if (!is_empty) ptr += copy_from_mem(ptr, keys.data(), keys_size_bytes);

  const bool is_ordered =
      flags_byte & (1 << theta_sketch_dup_alloc<A>::flags::IS_ORDERED);
  return compact_theta_sketch_dup_alloc<A>(is_empty, theta, std::move(keys),
                                           seed_hash, is_ordered);
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator
compact_theta_sketch_dup_alloc<A>::begin() const {
  return typename theta_sketch_dup_alloc<A>::const_iterator(keys_.data(),
                                                            keys_.size(), 0);
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator
compact_theta_sketch_dup_alloc<A>::end() const {
  return typename theta_sketch_dup_alloc<A>::const_iterator(
      keys_.data(), keys_.size(), keys_.size());
}

// builder

template <typename A>
update_theta_sketch_dup_alloc<A>::builder::builder()
    : lg_k_(DEFAULT_LG_K),
      rf_(DEFAULT_RESIZE_FACTOR),
      p_(1),
      seed_(DEFAULT_SEED) {}

template <typename A>
typename update_theta_sketch_dup_alloc<A>::builder&
update_theta_sketch_dup_alloc<A>::builder::set_lg_k(uint8_t lg_k) {
  if (lg_k < MIN_LG_K) {
    throw std::invalid_argument("lg_k must not be less than " +
                                std::to_string(MIN_LG_K) + ": " +
                                std::to_string(lg_k));
  }
  lg_k_ = lg_k;
  return *this;
}

template <typename A>
typename update_theta_sketch_dup_alloc<A>::builder&
update_theta_sketch_dup_alloc<A>::builder::set_resize_factor(resize_factor rf) {
  rf_ = rf;
  return *this;
}

template <typename A>
typename update_theta_sketch_dup_alloc<A>::builder&
update_theta_sketch_dup_alloc<A>::builder::set_p(float p) {
  p_ = p;
  return *this;
}

template <typename A>
typename update_theta_sketch_dup_alloc<A>::builder&
update_theta_sketch_dup_alloc<A>::builder::set_seed(uint64_t seed) {
  seed_ = seed;
  return *this;
}

template <typename A>
uint8_t update_theta_sketch_dup_alloc<A>::builder::starting_sub_multiple(
    uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf) {
  return (lg_tgt <= lg_min)
             ? lg_min
             : (lg_rf == 0) ? lg_tgt : ((lg_tgt - lg_min) % lg_rf) + lg_min;
}

template <typename A>
update_theta_sketch_dup_alloc<A>
update_theta_sketch_dup_alloc<A>::builder::build() const {
  return update_theta_sketch_dup_alloc<A>(
      starting_sub_multiple(lg_k_ + 1, MIN_LG_K, static_cast<uint8_t>(rf_)),
      lg_k_, rf_, p_, seed_);
}

// iterator

template <typename A>
theta_sketch_dup_alloc<A>::const_iterator::const_iterator(
    const std::pair<uint64_t, uint64_t>* keys, uint32_t size, uint32_t index)
    : keys_(keys), size_(size), index_(index) {
  while (index_ < size_ && keys_[index_].first == 0) ++index_;
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator&
theta_sketch_dup_alloc<A>::const_iterator::operator++() {
  do {
    ++index_;
  } while (index_ < size_ && keys_[index_].first == 0);
  return *this;
}

template <typename A>
typename theta_sketch_dup_alloc<A>::const_iterator
theta_sketch_dup_alloc<A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template <typename A>
bool theta_sketch_dup_alloc<A>::const_iterator::operator==(
    const const_iterator& other) const {
  return index_ == other.index_;
}

template <typename A>
bool theta_sketch_dup_alloc<A>::const_iterator::operator!=(
    const const_iterator& other) const {
  return index_ != other.index_;
}

template <typename A>
std::pair<uint64_t, uint64_t> theta_sketch_dup_alloc<A>::const_iterator::
operator*() const {
  return keys_[index_];
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(const std::string& value) {
  if (value.empty()) return;
  remove(value.c_str(), value.length());
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(uint64_t value) {
  remove(&value, sizeof(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(int64_t value) {
  remove(&value, sizeof(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(uint32_t value) {
  remove(static_cast<int32_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(int32_t value) {
  remove(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(uint16_t value) {
  remove(static_cast<int16_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(int16_t value) {
  remove(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(uint8_t value) {
  remove(static_cast<int8_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(int8_t value) {
  remove(static_cast<int64_t>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(double value) {
  union {
    int64_t long_value;
    double double_value;
  } long_double_union;

  if (value == 0.0) {
    long_double_union.double_value = 0.0;  // canonicalize -0.0 to 0.0
  } else if (std::isnan(value)) {
    long_double_union.long_value =
        0x7ff8000000000000L;  // canonicalize NaN using value from Java's
                              // Double.doubleToLongBits()
  } else {
    long_double_union.double_value = value;
  }
  remove(&long_double_union, sizeof(long_double_union));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(float value) {
  remove(static_cast<double>(value));
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::remove(const void* data,
                                              unsigned length) {
  HashState hashes;
  MurmurHash3_x64_128(data, length, seed_, hashes);
  const uint64_t hash =
      hashes.h1 >>
      1;  // Java implementation does logical shift >>> to make values positive
  internal_remove(hash);
}

template <typename A>
void update_theta_sketch_dup_alloc<A>::internal_remove(uint64_t hash) {
  this->is_empty_ = false;
  if (hash >= this->theta_ || hash == 0)
    return;  // hash == 0 is reserved to mark empty slots in the table
  if (hash_search_or_remove(hash, keys_.data(), lg_cur_size_)) {
    num_keys_--;
  }
}

template <typename A>
bool update_theta_sketch_dup_alloc<A>::hash_search_or_remove(
    uint64_t hash, std::pair<uint64_t, uint64_t>* table, uint8_t lg_size) {
  const uint32_t mask = (1 << lg_size) - 1;
  const uint32_t stride =
      update_theta_sketch_dup_alloc<A>::get_stride(hash, lg_size);
  uint32_t cur_probe = static_cast<uint32_t>(hash) & mask;
  const uint32_t loop_index = cur_probe;
  do {
    const uint64_t value = table[cur_probe].first;
    if (value == hash) {
      table[cur_probe].second--;
      if (table[cur_probe].second == 0) {
        table[cur_probe].first = 0;
        return true;
      }
      return false;
    }
    cur_probe = (cur_probe + stride) & mask;
  } while (cur_probe != loop_index);
  throw std::logic_error("key not found and search wrapped");
}

/*
 * aliases with default allocator for convenience (std::allocator<void> is no
 * longer supported in c++20)
 */
typedef theta_sketch_dup_alloc<std::allocator<void>> theta_sketch_dup;
typedef update_theta_sketch_dup_alloc<std::allocator<void>>
    update_theta_sketch_dup;
typedef compact_theta_sketch_dup_alloc<std::allocator<void>>
    compact_theta_sketch_dup;

// common helping functions

constexpr uint8_t log2(uint32_t n) { return (n > 1) ? 1 + log2(n >> 1) : 0; }

constexpr uint8_t lg_size_from_count(uint32_t n, double load_factor) {
  return log2(n) +
         ((n > static_cast<uint32_t>((1 << (log2(n) + 1)) * load_factor)) ? 2
                                                                          : 1);
}

// overlad ==
template<typename A>
bool operator==(theta_sketch_dup_alloc<A> const& l, theta_sketch_dup_alloc<A> const& r) {
  return l.isEqual(r);
}

template<typename A>
bool operator==(update_theta_sketch_dup_alloc<A> const& l, update_theta_sketch_dup_alloc<A> const& r) {
  return l.isEqual(r);
}

template<typename A>
bool operator==(compact_theta_sketch_dup_alloc<A> const& l, compact_theta_sketch_dup_alloc<A> const& r) {
  return l.isEqual(r);
}

} /* namespace datasketches */

#endif
