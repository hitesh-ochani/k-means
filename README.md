# k-means
Implemented k-means in Pyspark, using older way of computing union of shingles instead of min hash algorithm

I learned that even though I randomly initialized centroids, they kept converging to single centroid. Initializing should make a huge difference. My code was running for a really long time when I was using nested for loops iterating over all permutations of each hash function. I optimized the code so that as soon as signatures are filled it breaks the loop, this was a huge time saver
