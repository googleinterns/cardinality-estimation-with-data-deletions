#include <iostream>
#include <gtest/gtest.h>
#include <iomanip>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include <random>
#include "gen_string.h"
#include "theta_sketch_dup.hpp"

using namespace datasketches;

const int STREAM_LENGTH=1<<27;

int main() {
  for (int lg_k=7; lg_k<=7; lg_k++) {
    std::vector<double> estimations;
    gen_string gen;
    for (int j=0; j<100; j++) {
      auto a=update_theta_sketch_dup::builder().set_lg_k(lg_k).build();
      for (int i=0; i<STREAM_LENGTH; i++) a.update(gen.next());
      estimations.push_back(a.get_estimate());
    }
    std::ofstream output("test_data/2_to_27/lgk="+std::to_string(lg_k),std::ofstream::out);
    for (auto e:estimations) output << e << std::endl;
    output.close();
  }
  // auto serialized_bytes = a.serialize();
  // std::ofstream output("data",std::ofstream::out);
  // for (auto e:serialized_bytes) {
  //   output << e << " ";
  // }
  // std::cout << "------------------------------------------------------" << std::endl;
  // std::cout << "Estimation of the cardinality is: " << a.get_estimate() << std::endl;
  // std::cout << "------------------------------------------------------" << std::endl;
  return 0;
}

