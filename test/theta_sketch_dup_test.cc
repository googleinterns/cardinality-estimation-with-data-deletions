#include <iostream>
#include <gtest/gtest.h>
#include <iomanip>
#include <fstream>
#include <random>
#include "theta_sketch_dup.hpp"

namespace datasketches {

TEST(THETA_SKETCH_DUP, TEST_SERIALIZE_DETERMINED_CASE_WITHOUT_ESTIMATION) {
  auto a=update_theta_sketch_dup::builder().set_lg_k(5).build();
  for (int i=0; i<20; i++) a.update(i);
  auto serialized_bytes = a.serialize();
  auto b=update_theta_sketch_dup::deserialize(serialized_bytes.data(), serialized_bytes.size());
  ASSERT_EQ(a.get_estimate(),b.get_estimate());
  for (int i=0; i<10; i++) {
    b.remove(i);
  }
  ASSERT_EQ(b.get_estimate(),10);
}

TEST(THETA_SKETCH_DUP, TEST_SERIALIZE_UNDERTERMINED_CASE_UDNER_ESTIMATION) {
  auto a=update_theta_sketch_dup::builder().set_lg_k(10).build();
  for (int i=0; i<1000000; i++) a.update(i);
  auto serialized_bytes = a.serialize();
  auto b=update_theta_sketch_dup::deserialize(serialized_bytes.data(), serialized_bytes.size());
  ASSERT_EQ(a.get_estimate(),b.get_estimate());
  // TODO: elements compare between a and b: maybe overload ASSERT_EQ for class theta_sketch_dup, updata_theta_sketch_dup, compact_theta_sketch_dup
}

}
