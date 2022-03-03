# Overview
This file contains some preliminary notes on KNN, distance metrics, and how to do both with categorical as well as numeric attributes. This information is all from CSC 466 notes and lab work.
<br>
# KNN
## General Concept
Classify records by taking the plurality (most common) class of the k nearest neighbors.<br>
This requires being able to calculate the distance between records, regardless of categorical or numeric attributes.
## High-Level Pseudocode
1. Accept a set of labeled training data.
2. Given a new record to classify:
3. Compute the distance between the new record and *all* other records in the training set (i.e. in the model).
4. Determine the k nearest neighbors (i.e. shortest distances) to the record under consideration.
5. Predict the class of the new record is the plurality class of these k neighbors.
## Distance Metrics
### Numeric Data
* euclidean: "direct line"
  * `sqrt(sum((r1_i - r2_i) ** 2))`
* mahattan: "city block" or "L" traversal
  * `sum(abs(r1_i - r2_i))`
* cosine *similarity* (so do `1-cos_sim` to get the *distance*)
  * vector interpretation: `(r1 dot r2) / (norm(r1) * norm(r2))`
  * practical internpretation: `sum(r1_i * r2_i) / (sum(r1_i ^ 2) * sum(r2_i ^ 2))`
### Categorical Data
* anti-dice: based on finding the number of mismatches in categorical attributes
  * returns `(# mismatches) / (total # categorical attributes)`
### Merging Distances
* Combine the numeric and categorical distance components using a weighted sum (i.e. linear combination) where the weights are the proportion of a full record that the numeric or categorical attributes compose.
* For example, if a record contains 7 attributes total with 3 numeric and 4 categorical, the overall distance would be `3/7 * numeric_dist + 4/7 * catg_dist`
## Methods to Convert Continuous Numeric Attributes to Categorical
1. Bucketing: establish ranges (buckets) across the whole numeric range and assign categorical labels to each bucket.
  2. Ex: age --> child = 0-18, young_adult = 18-25, prime = 25-40, middle_age = 40-65, senior = 65+
## Other Data Considerations
* numeric data standardization (otherwise attributes with larger ranges can overshadow attributes with smaller ranges, even though the relative distance of the smaller-ranged attribute may be greater).
# Proposed Distributed Implementation Concept
1. Compute distance from new record to each data point.
2. Sort distances in ascending order.
3. Take the top k elements.
4. Reduce by taking the plurality class.
