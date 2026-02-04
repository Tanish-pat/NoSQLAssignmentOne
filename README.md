## SimuFragDB: Distributed RDBMS Simulator

A simulated distributed database system running on a single physical machine, that emulates a multi-node environment by horizontal fragmentation across multiple PostgreSQL instances.

---

### **Objectives**

**Horizontal Fragmentation**: Partitioning relations into subsets of tuples across N database fragments.

**Deterministic Routing**: Using a hash-based routing function to direct queries to specific nodes based on the Primary Key.

**Data Aggregation**: Implementing client-side logic to aggregate results (like averages and maximums) from multiple fragments.

**Accuracy Verification**: Comparing distributed execution output against a single-instance baseline to evaluate data consistency.

---

