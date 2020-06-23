#include "theta_intersection.hpp"
#include <gtest/gtest.h>

namespace datasketches {

TEST(Intersection, Exact) {
  theta_intersection inter;
  auto a = update_theta_sketch::builder().set_lg_k(15).build();
  auto b = update_theta_sketch::builder().set_lg_k(15).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  inter.update(a);
  inter.update(b);
  compact_theta_sketch result = inter.get_result();
  ASSERT_FALSE(result.is_empty());
  ASSERT_FALSE(result.is_estimation_mode());
  ASSERT_EQ(result.get_estimate(), 8000);
}

TEST(Intersection, Estimation) {
  theta_intersection inter;
  auto a = update_theta_sketch::builder().set_lg_k(12).build();
  auto b = update_theta_sketch::builder().set_lg_k(12).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  inter.update(a);
  inter.update(b);
  compact_theta_sketch result = inter.get_result();
  ASSERT_FALSE(result.is_empty());
  ASSERT_TRUE(result.is_estimation_mode());
  EXPECT_NEAR(result.get_estimate(), 8000, 200);
}

}  // namespace datasketches
