load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "theta",
    hdrs = [
        "theta/binomial_bounds.hpp",
        "theta/theta_a_not_b.hpp",
        "theta/theta_a_not_b_impl.hpp",
        "theta/theta_intersection.hpp",
        "theta/theta_intersection_impl.hpp",
        "theta/theta_sketch.hpp",
        "theta/theta_sketch_impl.hpp",
        "theta/theta_union.hpp",
        "theta/theta_union_impl.hpp",
        "common/MurmurHash3.h",
        "common/bounds_binomial_proportions.hpp",
        "common/common_defs.hpp",
        "common/count_zeros.hpp",
        "common/inv_pow2_table.hpp",
        "common/memory_operations.hpp",
        "common/serde.hpp",
    ],
    visibility = ["//:__pkg__","//theta_dup:__pkg__","//test:__pkg__"],
)
