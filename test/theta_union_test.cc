#include "theta_union.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>
#include <iostream>

namespace datasketches {

TEST(Union, Exact) {
  theta_union u = theta_union::builder().set_lg_k(15).build();
  auto a = update_theta_sketch::builder().set_lg_k(15).build();
  auto b = update_theta_sketch::builder().set_lg_k(15).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  u.update(a);
  u.update(b);
  compact_theta_sketch result = u.get_result();
  EXPECT_FALSE(result.is_empty());
  EXPECT_FALSE(result.is_estimation_mode());
  EXPECT_EQ(result.get_estimate(), 12000);
}

TEST(Union, Estimation) {
  theta_union u = theta_union::builder().build();
  auto a = update_theta_sketch::builder().set_lg_k(12).build();
  auto b = update_theta_sketch::builder().set_lg_k(12).build();
  for (int i = 0; i < 10000; i++) a.update(i);
  for (int i = 0; i < 10000; i++) b.update(i + 2000);
  u.update(a);
  u.update(b);
  compact_theta_sketch result = u.get_result();
  EXPECT_FALSE(result.is_empty());
  EXPECT_TRUE(result.is_estimation_mode());
  EXPECT_NEAR(result.get_estimate(), 12000, 200);
}

}  // namespace datasketches
