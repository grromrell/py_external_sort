### External Sorting

Have a file that won't fit in memory but you need to sort it? Stuck using Athena/PrestoDB and need to sort stuff? Me too, that is why this is exists. Uses timsort for in-memory and merge sorts for on-disk. Heavily influenced by the work of  Richard Penman on csvsort. Why make a new version? Well a couple of reasons:

* csvsort does not work with Python3
* Reads and writes are handled exclusively by the csv library, causing some un-needed overhead in the inital split of the files
* Uses sys.getsizeof to determine whether the file is the right length, which does not map to size on disk, thus max_size parameter is often doubled in reality
* Rewrites the whole file to add the header at the end
* Is a single process (aka slow)
* Uses mercurial (which is why this isn't simply a fork)

So I decided to not do the above things. The last point is the killer. In order to make this bad boy (kinda) fast we need to use some multiprocessing, which in reality requires a full rewrite, not just a pull request. So just how fast is it?

* A sort of a 20 gb file with this version can be done in ~1860 seconds on average (31 minutes) and that can be increased if parameters are optimized
* A sort of a 20 gb file with csvsort 1.3 finished in ~19006 seconds on average (316 minutes) using the same parameters and files.

Wow, its way faster! 10x isn't bad. Good luck.
