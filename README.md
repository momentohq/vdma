# vdma
A Valkey module for terabit/s-class transfers using DMA.

This module uses one-sided RDMA via [libfabric](https://ofiwg.github.io/libfabric/)
to do zero-copy, GIL-offloaded transfers into and out of Valkey's managed memory.
It supports the `efa-direct` libfabric provider to enable the fastest transfers,
and open the door to GpuDirect with memory from Valkey transferring straight into
gpu memory.

`vdma` uses the standard valkey string data type. You can `dma.set` a key, `get` it,
and vice versa.

* [Design](./design/Design.md)
* [Valkey](https://valkey.io)
* [AWS EFA (Elastic Fabric Adapter)](https://aws.amazon.com/hpc/efa/)

# dma-libfabric
A server-side library for writing modules that use DMA.
