GitHub Flavored Markdown
================================

*View the [source of this content](http://github.github.com/github-flavored-markdown/sample_content.html).*

Let's get the whole "linebreak" thing out of the way. The next paragraph contains two phrases separated by a single newline character:

Roses are red
Violets are blue

The next paragraph has the same phrases, but now they are separated by two spaces and a newline character:

Roses are red  
Violets are blue

Oh, and one thing I cannot stand is the mangling of words with multiple underscores in them like perform_complicated_task or do_this_and_do_that_and_another_thing.

A bit of the GitHub spice
-------------------------

This framework based on two main concepts: 
1. KeyValueDataSet - represents the sequence file on hdfs, controls its data and provide usefull method to manipulate it
2. Operation - operation which transform on KeyValueDataSet to another (Map-, Reduce-, or MapReduce- Job).

The main goal of this framework is to abstract from the hadop routines like: Jobs, Writables, Configuration, IO, etc.




