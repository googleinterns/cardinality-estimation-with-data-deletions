#include "theta_a_not_b.hpp"
#include <gtest/gtest.h>

namespace datasketches {

TEST(ANotBTest, Exact) {
  theta_a_not_b a_not_b;
  auto a = update_theta_sketch::builder().set_lg_k(15).build();
  auto b = update_theta_sketch::builder().set_lg_k(15).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  compact_theta_sketch result = a_not_b.compute(a, b);
  EXPECT_FALSE(result.is_empty());
  EXPECT_FALSE(result.is_estimation_mode());
  EXPECT_EQ(result.get_estimate(), 2000);
}

TEST(ANotBTest, Estimation) {
  theta_a_not_b a_not_b;
  auto a = update_theta_sketch::builder().set_lg_k(12).build();
  auto b = update_theta_sketch::builder().set_lg_k(12).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  compact_theta_sketch result = a_not_b.compute(a, b);
  EXPECT_FALSE(result.is_empty());
  EXPECT_TRUE(result.is_estimation_mode());
  EXPECT_NEAR(result.get_estimate(), 2000, 200);
}

}  // namespace datasketches
