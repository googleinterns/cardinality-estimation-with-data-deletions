#include <iostream>
#include <gtest/gtest.h>
#include <iomanip>
#include <fstream>
#include "theta_a_not_b.hpp"

namespace datasketches {

TEST(A_NOT_B_TEST, EXACT) {
    theta_a_not_b a_not_b;
    auto a=update_theta_sketch::builder().set_lg_k(15).build();
    auto b=update_theta_sketch::builder().set_lg_k(15).build();
    for (int i=0; i<10000; i++)
        a.update(i);
    for (int i=0; i<10000; i++)
        b.update(i+2000);
    compact_theta_sketch result = a_not_b.compute(a,b);
    // std::cout << a.to_string() << b.to_string();
    // std::cout << result.to_string();
    std::cout << "cardinality of a_not_b: " << result.get_estimate() << std::endl;
    ASSERT_FALSE(result.is_empty());
    ASSERT_FALSE(result.is_estimation_mode());
    ASSERT_EQ(result.get_estimate(),2000);
}

TEST(A_NOT_B_TEST, ESTIMATION) {
    theta_a_not_b a_not_b;
    auto a=update_theta_sketch::builder().set_lg_k(12).build();
    auto b=update_theta_sketch::builder().set_lg_k(12).build();
    for (int i=0; i<10000; i++)
        a.update(i);
    for (int i=0; i<10000; i++)
        b.update(i+2000);
    compact_theta_sketch result = a_not_b.compute(a,b);
    // std::cout << a.to_string() << b.to_string();
    // std::cout << result.to_string();
    std::cout << "cardinality of a_not_b: " << result.get_estimate() << std::endl;
    ASSERT_FALSE(result.is_empty());
    ASSERT_TRUE(result.is_estimation_mode());
    ASSERT_NEAR(result.get_estimate(),2000,200);
}

}
