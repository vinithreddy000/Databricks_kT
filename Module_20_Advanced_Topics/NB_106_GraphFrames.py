# Databricks notebook source
# DBTITLE 1,Section 1-2: Overview
# MAGIC %md
# MAGIC # Notebook 106: GraphFrames (Graph Processing)
# MAGIC ## Module 20: Advanced Topics
# MAGIC ## Language: Python (PySpark)
# MAGIC ## Estimated time: 40 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 1 — What Is This?
# MAGIC
# MAGIC ### Plain English
# MAGIC **GraphFrames** brings graph processing to Spark. Graphs model **relationships** between entities: social networks (who knows who), supply chains (which factory supplies which warehouse), fraud detection (which accounts are connected). GraphFrames runs distributed graph algorithms (PageRank, shortest paths, connected components) on billions of edges.
# MAGIC
# MAGIC ### Real-World Analogy
# MAGIC A **city road map**: cities are **vertices** (nodes), roads between them are **edges**. Graph algorithms answer questions like: "What's the shortest route?" (shortest path), "Which city is most connected?" (PageRank), "Which cities form isolated clusters?" (connected components).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## SECTION 2 — How It Works
# MAGIC
# MAGIC ```
# MAGIC Graph = Vertices (nodes) + Edges (connections)
# MAGIC
# MAGIC   Vertices DataFrame:         Edges DataFrame:
# MAGIC   | id   | name    |          | src  | dst  | relationship |
# MAGIC   | "a"  | "Alice" |          | "a"  | "b"  | "follows"    |
# MAGIC   | "b"  | "Bob"   |          | "b"  | "c"  | "follows"    |
# MAGIC   | "c"  | "Carol" |          | "a"  | "c"  | "friend"     |
# MAGIC
# MAGIC   g = GraphFrame(vertices_df, edges_df)
# MAGIC   g.pageRank(resetProbability=0.15, maxIter=10)  # Who's most important?
# MAGIC   g.shortestPaths(landmarks=["a"])               # Distance from node a.
# MAGIC   g.connectedComponents()                        # Find clusters.
# MAGIC   g.triangleCount()                              # Find tightly connected groups.
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Sections 3-7: GraphFrames Examples and Homework
# ═══════════════════════════════════════════════════════════════════
# SECTIONS 3-7 — GRAPHFRAMES
# ═══════════════════════════════════════════════════════════════════

from pyspark.sql.functions import col  # Spark functions.

print("="*70)
print("SECTIONS 3-7: GraphFrames")
print("="*70)

# ─── EXAMPLE 1: Create a graph ───
print("\n" + "-"*60)
print("EXAMPLE 1: Creating a GraphFrame")
print("-"*60)

try:
    from graphframes import GraphFrame  # GraphFrame import.
    
    # Vertices: people in a social network.
    vertices = spark.createDataFrame([
        ("alice", "Alice", 34), ("bob", "Bob", 36),
        ("charlie", "Charlie", 30), ("diana", "Diana", 29),
        ("eve", "Eve", 32)
    ], ["id", "name", "age"])  # Must have 'id' column.
    
    # Edges: who follows whom.
    edges = spark.createDataFrame([
        ("alice", "bob", "follows"), ("bob", "charlie", "follows"),
        ("charlie", "diana", "follows"), ("diana", "alice", "follows"),
        ("eve", "alice", "follows"), ("eve", "bob", "follows")
    ], ["src", "dst", "relationship"])  # Must have 'src' and 'dst'.
    
    # Create GraphFrame.
    g = GraphFrame(vertices, edges)  # Vertices + Edges = Graph.
    print(f"\n  Vertices: {g.vertices.count()}, Edges: {g.edges.count()}")
    print("\n  Vertices:")
    display(g.vertices)  # display() for output.
    print("  Edges:")
    display(g.edges)  # display() for output.
    
    # ─── EXAMPLE 2: PageRank ───
    print("\n" + "-"*60)
    print("EXAMPLE 2: PageRank (who is most influential?)")
    print("-"*60)
    
    pr = g.pageRank(resetProbability=0.15, maxIter=10)  # Run PageRank.
    print("\n  PageRank scores (higher = more influential):")
    display(pr.vertices.select("id", "name", "pagerank").orderBy(col("pagerank").desc()))
    
    # ─── EXAMPLE 3: Shortest Paths ───
    print("\n" + "-"*60)
    print("EXAMPLE 3: Shortest Paths")
    print("-"*60)
    
    sp = g.shortestPaths(landmarks=["alice"])  # Distance from alice.
    print("\n  Shortest paths to 'alice':")
    display(sp.select("id", "distances"))

except ImportError:
    print("\n  GraphFrames not installed. Install with:")
    print("  %pip install graphframes")
    print("  Or add graphframes:graphframes:0.8.3-spark3.5-s_2.12 as Maven library.")
except Exception as e:
    print(f"\n  GraphFrames error: {e}")
    print("  (May need checkpoint directory: spark.sparkContext.setCheckpointDir('/tmp/graphframes'))")

# ─── HOMEWORK ───
print("\n" + "="*70)
print("HOMEWORK — GraphFrames")
print("="*70)
print("  Level 1-3: Create vertices+edges, make GraphFrame.")
print("  Level 4-6: Run PageRank, shortest paths, connected components.")
print("  Level 7-10: Motif finding (g.find('(a)-[e]->(b)')), triangle count.")
print("  Use cases: social networks, fraud rings, supply chain analysis.")
print("\n" + "="*70)
print("✓ HOMEWORK COMPLETED — Notebook 106")
print("="*70)