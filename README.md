# Memstore  ![Travis CI Build Status](https://api.travis-ci.org/mngharbi/memstore.svg?branch=master) [![Coverage](http://gocover.io/_badge/github.com/mngharbi/memstore)](http://gocover.io/github.com/mngharbi/memstore)

Memstore is an in-memory, thread safe, multiple-key datastore for Go.

## Overview

The datastore is built on top of multiple Left-Leaning Red-Black trees.

It allows you to store a collection of any arbitrary Go language structures, as long as you define a method to define comparison for arbitrary indexes.

It provides a way to get ranges based on any index in O(k + log n) time, k being the number of elements retrieved, and n being the number of structures in the datastore.

Also, getting minimum and maximum values based on any index defined runs O(log n).

All methods exported are thread safe, and enable multiple readers through a native Read Write Lock.

It's meant for use as a light-weight, efficient in-memory datastore as part of your Go package. If you want to persist data or advanced features (transactions, detailed search...etc), this may not not be ideal.

## Installation

With a healthy Go Language installed, simply run `go get github.com/mngharbi/memstore`


## Dependency

This package depends on [GoLLRB](https://github.com/petar/GoLLRB), built by [Petar Maymounkov](http://pdos.csail.mit.edu/~petar/).

However, I had to patch it to leverage it for this package. So, this package technically depends on [the forked version](https://github.com/mngharbi/GoLLRB).


