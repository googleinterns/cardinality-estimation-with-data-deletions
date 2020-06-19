#include "theta_sketch_dup.h"
#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <set>
#include <string>
#include "gen_string.h"

namespace datasketches {

TEST(THETA_SKETCH_DUP, TEST_SERIALIZE_DETERMINED_CASE_WITHOUT_ESTIMATION) {
  auto a = update_theta_sketch_dup::builder().set_lg_k(5).build();
  for (int i = 0; i < 20; i++) a.update(i);
  auto serialized_bytes = a.serialize();
  auto b = update_theta_sketch_dup::deserialize(serialized_bytes.data(),
                                                serialized_bytes.size());
  ASSERT_EQ(a.get_estimate(), b.get_estimate());
  for (int i = 0; i < 10; i++) {
    b.remove(i);
  }
  ASSERT_EQ(b.get_estimate(), 10);
}

TEST(THETA_SKETCH_DUP, TEST_SERIALIZE_UNDERTERMINED_CASE_UDNER_ESTIMATION) {
  auto a = update_theta_sketch_dup::builder().set_lg_k(10).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  auto serialized_bytes = a.serialize();
  auto b = update_theta_sketch_dup::deserialize(serialized_bytes.data(),
                                                serialized_bytes.size());
  ASSERT_EQ(a,b);
}

TEST(THETA_SKETCH_DUP, TEST_STRING_STREAM) {
  auto a = update_theta_sketch_dup::builder().set_lg_k(10).build();
  gen_string gen;
  for (int i = 0; i < 1000000; i++) a.update(gen.next());
  EXPECT_NEAR(a.get_estimate(), 1000000, 10000);
}

TEST(THETA_SKETCH_DUP, TEST_GAUSSIAN_INPUT) {
  std::ofstream output("data", std::ofstream::out);
  int stddev = 100000;
  auto a = update_theta_sketch_dup::builder().set_lg_k(12).build();
  std::random_device rd{};
  std::mt19937 gen{rd()};
  std::normal_distribution<> d{0, static_cast<double>(stddev)};
  std::set<int> S;
  while (S.size() < 100000) {
    int x = std::round(d(gen));
    a.update(x);
    S.insert(x);
  }
  ASSERT_NEAR(a.get_estimate(), S.size(), 2000);
}

}  // namespace datasketches
