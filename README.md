## SimuFragDB: Distributed RDBMS Simulator

A simulated distributed database system running on a single physical machine, that emulates a multi-node environment by horizontal fragmentation across multiple PostgreSQL instances.

---

### **Objectives**

**Horizontal Fragmentation**: Partitioning relations into subsets of tuples across N database fragments.

**Deterministic Routing**: Using a hash-based routing function to direct queries to specific nodes based on the Primary Key.

**Data Aggregation**: Implementing client-side logic to aggregate results (like averages and maximums) from multiple fragments.

**Accuracy Verification**: Comparing distributed execution output against a single-instance baseline to evaluate data consistency.

---

### **Usage**

Ensure PostgreSQL is installed. The credentials `DB_USER`, `DB_PASSWORD` and `NUM_FRAGMENTS` need to be configured in `Driver.java`.

```bash
mvn clean compile exec:java -Dexec.mainClass="Driver"
```

The output will be saved in `output/`.

---

### **Authors**

* [IMT2022021 Rutul Patel](https://github.com/RutulPatel007)
* [IMT2022049 Tanish Pathania](https://github.com/Tanish-Pat)
* [IMT2022076 Mohit Naik](https://github.com/mohitiiitb)
* [IMT2022086 Ananthakrishna K](https://github.com/Ananthakrishna-K-13)