load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_test(
    name = "theta_sketch_dup",
    srcs = glob(["theta_sketch_dup_test.cc"]),
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
        "-Itheta_dup/include",
        "-Iutils",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
        "//theta_dup:theta_dup",
        "//utils:utils",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)

cc_test(
    name = "theta_sketch_set",
    srcs = glob(["theta_a_not_b_test.cc", "theta_intersection_test.cc", "theta_union_test.cc"]),
    copts = [
        "-Ithird_party/incubator-datasketches-cpp/theta",
        "-Ithird_party/incubator-datasketches-cpp/common",
        "-Itheta_dup/include",
        "-Iutils",
    ],
    deps = [
        "//third_party/incubator-datasketches-cpp:theta",
        "//theta_dup:theta_dup",
        "//utils:utils",
        "@googletest//:gtest",
        "@googletest//:gtest_main",
    ],
)
