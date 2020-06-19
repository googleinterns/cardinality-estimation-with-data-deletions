load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "theta_dup",
    hdrs = [
        "include/theta_sketch_dup.h",
        "include/utils.h",
    ],
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
    ],
    visibility = ["//:__pkg__","//test:__pkg__",],
)
