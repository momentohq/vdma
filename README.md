# vdma
A Valkey module for DMA.

This module uses one-sided RDMA via [libfabric](https://ofiwg.github.io/libfabric/)
to do zero-copy, GIL-offloaded transfers into and out of Valkey's managed memory.
It supports the `efa-direct` libfabric provider to enable the fastest transfers,
and open the door to GpuDirect with memory from Valkey transferring straight into
gpu memory.

* [Design](./design/Design.md)
* [Valkey](https://valkey.io)
* [AWS EFA (Elastic Fabric Adapter)](https://aws.amazon.com/hpc/efa/)

