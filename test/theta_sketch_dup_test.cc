#include <iostream>
#include <gtest/gtest.h>
#include <iomanip>
#include <fstream>
#include "theta_sketch_dup.hpp"

namespace datasketches {

TEST(THETA_SKETCH_DUP, TEST) {
    auto a=update_theta_sketch_dup::builder().set_lg_k(12).build();
    for (int i=0; i<1000000; i++) a.update(i);
    for (int i=0; i<10; i++) {
      a.remove(0);
      // std::cout << a.get_estimate() << std::endl;
    }
    std::cout << a.to_string();
    // std::cout << result.to_string();
    std::cout << "cardinality of a_dup: " << a.get_estimate() << std::endl;
}

}
