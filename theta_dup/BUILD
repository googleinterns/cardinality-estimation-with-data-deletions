load("@rules_cc//cc:defs.bzl", "cc_library")

cc_library(
    name = "theta_dup",
    srcs = [
        "include/theta_sketch_dup.pb.cc",
    ],
    hdrs = [
        "include/theta_sketch_dup.h",
        "include/utils.h",
        "include/theta_sketch_dup.pb.h",
    ],
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
        "-I/usr/local/include",
    ],
    linkopts = [
        "-pthread",
        "-L/usr/local/lib",
        "-lprotobuf",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
    ],
    visibility = ["//:__pkg__","//test:__pkg__",],
)
