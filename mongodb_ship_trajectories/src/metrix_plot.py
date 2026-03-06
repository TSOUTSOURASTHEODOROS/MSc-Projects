import matplotlib.pyplot as plt

# ta apotelesma ta tun run ( msm ) sta queria pou etrexa sto compass gia an ftixw to plot

queries = ["Q1", "Q2", "Q3", "Q4"]
times_ms = [7086, 5149, 4538, 3151]

plt.figure()
plt.bar(queries, times_ms)
plt.xlabel("Query")
plt.ylabel("Execution time (ms)")
plt.title("Execution time per query (MongoDB Compass Explain Plan)")
plt.tight_layout()
plt.savefig("q_times.png", dpi=200)
print("Saved: q_times.png")
