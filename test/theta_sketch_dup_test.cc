#include "theta_sketch_dup.h"
#include <gtest/gtest.h>
#include <random>
#include <set>
#include <string>
#include "gen_string.h"

namespace datasketches {

TEST(ThetaSketchDup, TestSerializeDeterminedCaseWithoutEstimation) {
  // @a: theta sketch with storage set to be [2^5, 2^5*threshold)
  // @b: theta sketch deserialized from serialized a 
  // this test data stream only has 20 distinct elements, so the result is exact
  // instead to estimated
  auto a = update_theta_sketch_dup::builder().set_lg_k(5).build();
  for (int i = 0; i < 20; i++) a.update(i);
  
  // serialize using proto
  // a and b should be the same before/after the serialization
  datasketches_pb::ThetaSketchDup serialized_proto;
  a.serialize(&serialized_proto);
  auto b = update_theta_sketch_dup::deserialize(serialized_proto);
  EXPECT_EQ(a, b);

  // serialize using bytes
  // a and b should be the same before/after the serialization
  auto serialized_bytes = a.serialize();
  b = update_theta_sketch_dup::deserialize(serialized_bytes.data(),
                                           serialized_bytes.size());
  EXPECT_EQ(a, b);

  // remove 10 elements, the sketch has 10 remaining elements
  for (int i = 0; i < 10; i++) {
    b.remove(i);
  }
  EXPECT_EQ(b.get_estimate(), 10);
}

TEST(ThetaSketchDup, TestSerializeUnderterminedCaseUnderEstimation) {
  // @a: theta sketch with storage set to be [2^10, 2^10*threshold)
  // @b: theta sketch deserialized from serialized a
  // this test data stream has 10000 distinct elements, which exceeds the
  // storage size, so the cardinality is estimated instead of exact
  auto a = update_theta_sketch_dup::builder().set_lg_k(10).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  
  // serialize using proto
  // a and b should be the same before/after the serialization
  datasketches_pb::ThetaSketchDup serialized_proto;
  a.serialize(&serialized_proto);
  auto b = update_theta_sketch_dup::deserialize(serialized_proto);
  EXPECT_EQ(a, b);

  for (int i = 5000; i < 20000; i++) a.update(i);
  for (int i = 5000; i < 20000; i++) b.update(i);
  EXPECT_EQ(a, b);

  // serialize using bytes
  // a and b should be the same before/after the serialization
  auto serialized_bytes = a.serialize();
  b = update_theta_sketch_dup::deserialize(serialized_bytes.data(),
                                                serialized_bytes.size());
  EXPECT_EQ(a, b);
}

TEST(ThetaSketchDup, TestStringStream) {
  // @a: theta sketch with storage set to be [2^15 2^15*threshold)
  // @gen: random string generator
  // input data stream are random strings with random length
  auto a = update_theta_sketch_dup::builder().set_lg_k(15).build();
  gen_string gen;
  for (int i = 0; i < 1000000; i++) a.update(gen.next());
  // expect the estimation near exact value with 2% accuarcy
  EXPECT_NEAR(a.get_estimate(), 1000000, 20000);
}

TEST(ThetaSketchDup, TestGaussianInput) {
  // @a: theta sketch with storage set to be [2^15, 2^15*threshold)
  // @stddev: standard deviation of the gaussian dist
  // @gen: random number generator
  // @d: normal distribution with standard deviation stddev
  // @current_distinct_elements: 
  // input data stream are random strings with random length
  int stddev = 1000000;
  auto a = update_theta_sketch_dup::builder().set_lg_k(15).build();
  std::random_device rd{};
  std::mt19937 gen{rd()};
  std::normal_distribution<> d{0, static_cast<double>(stddev)};
  std::set<int> current_distinct_elements;
  while (current_distinct_elements.size() < 100000) {
    int x = std::round(d(gen));
    a.update(x);
    current_distinct_elements.insert(x);
  }
  // expect the estimation near exact value with 2% accuarcy
  EXPECT_NEAR(a.get_estimate(), current_distinct_elements.size(), 2000);
}

}  // namespace datasketches
