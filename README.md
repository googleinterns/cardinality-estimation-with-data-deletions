Disclaimer: This is not an officially supported Google product.

# Sketch-Based Cardinality Estimation with Data Deletions

The cardinality of a set is a measure of the "number of distinct elements
of the set". For example, the set contains 3 different elements, and therefore
has a cardinality of 3.

Estimating the number of distinct elements in a data set or data stream with
repeated elements is known as the cardinality estimation problem, or the
count-distinct problem.

One important category of the existing cardinality estimation algorithms is
sketch-based, where each element in the stream is usually hashed into a data
structure called a “sketch” and the sketch can provide the estimate number of
distinct elements at query time.

Most sketches are based on streaming model where data deletions are not handled.
Designing a Approx Count Distinct sketch supporting deletions, or more generally
set intersections and differences while keeping the sketch/aggregation state
sizes reasonably small is quite a hard problem.

The goal of this project is to review, experiment and potentially improve the
sketch-based cardinality estimation algorithms supporting deletions.

