load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "theta_dup",
    hdrs = [
        "include/theta_sketch_dup.hpp",
        "include/theta_sketch_dup_impl.hpp",
        "include/utils.hpp",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
    ],
    visibility = ["//:__pkg__","//test:__pkg__",],
)
