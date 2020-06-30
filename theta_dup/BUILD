load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "theta_sketch_dup_proto",
    srcs = [
        "include/theta_sketch_dup.proto",
    ],
)

cc_proto_library(
    name = "theta_sketch_dup_cc_proto",
    deps = [
        ":theta_sketch_dup_proto",
    ],
)

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
        ":theta_sketch_dup_cc_proto",
    ],
    visibility = ["//:__pkg__","//test:__pkg__",],
)
