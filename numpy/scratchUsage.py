import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

time, value = np.loadtxt("diskOpsReadSorted.csv.new", unpack=True, delimiter=',',
        converters={ 0: mdates.strpdate2num('%Y-%m-%d %H:%M')})

print "Median :-", np.median(value)
print "Min :-",np.min(value)
print "Max :-",np.max(value)
print "Std :-",np.std(value)
print "Mean :-",np.mean(value)
print "P95 :-",np.percentile(value,95)

plt.plot_date(x=time, y=value,fmt="g-")
plt.title("Disk Read Ops")
plt.ylabel("Count")
plt.grid(True)
plt.show()
