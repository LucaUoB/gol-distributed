import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

benchmark_data = pd.read_csv('results.csv', header=0, names=['name', 'time', 'range'])

benchmark_data['time'] /= 1e+9
benchmark_data['workers'] = benchmark_data['name'].str.extract('(\d+)_')

ax = sns.barplot(data=benchmark_data, x='workers', y='time')
ax.set(xlabel='Workers', ylabel='Time taken (s)')
plt.show()

# -------------------------Benchmarking Commands-------------------------

# go test -run ^$ -bench . -benchtime 1x -count 5 | tee results.out
# go run golang.org/x/perf/cmd/benchstat -csv results.out | tee results.csv
# python plot.py
