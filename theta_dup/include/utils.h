#ifndef UTILS_H_
#define UTILS_H_

#include <cinttypes>
#include <cmath>
#include <iostream>
#include <type_traits>

namespace datasketches {

// overload < for std::pair<A, int64_t>
template <typename A>
bool operator<(const std::pair<A, int64_t>& lhs, const std::pair<A, int64_t>& rhs) {
  return lhs.first < rhs.first;
}

// overload << for std::pair<A, int64_t>
template <typename A>
std::ostream& operator<<(std::ostream& os, const std::pair<A, int64_t>& obj) {
  return os << "(" << obj.first << "," << obj.second << ")";
}

} /* namespace datasketches */

#endif
