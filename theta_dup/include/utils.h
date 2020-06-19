#ifndef UTILS_H_
#define UTILS_H_

#include <cinttypes>
#include <cmath>
#include <iostream>
#include <type_traits>

template <typename A>
bool operator<(const std::pair<A, A>& lhs, const std::pair<A, A>& rhs) {
  return lhs.first < rhs.first;
}

template <typename A>
std::ostream& operator<<(std::ostream& os, const std::pair<A, A>& obj) {
  return os << "(" << obj.first << "," << obj.second << ")";
}

#endif
