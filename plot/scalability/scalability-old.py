import matplotlib.pyplot as plt

# Measurements from throughput.data

# Measurements from latency.data



def do_plot():
    f, ax = plt.subplots(2,1, figsize=(6,5))
    replicaNo = [4, 8, 16, 32]
    xticks = [0,4, 8, 16, 32, 40]
    xticks_label = ["","4", "8", "16", "32", "64", ""]
    thru = [
    ('HotStuff',[
        [153.2, 153.3],
        [133.5, 133.8],
        [94.95, 95],
        [55.9, 55.1],
    ], '-o', 'coral'),
    ('2CHS',[
        [154.3, 154.5],
        [133.4, 133.7],
        [94.5, 94.9],
        [55.6, 55.8],
    ], '-^', 'darkseagreen'),
    ('Streamlet',[
        [152.4, 152.5],
        [131.9, 132.2],
        [95.7, 95.9],
        [60.1, 60.3],
    ], '-s', 'steelblue')
    ]
    for name, entries, style, color in thru:
        thru = []
        errs = []
        for item in entries:
            thru.append((item[0]+item[1])/2.0)
            errs.append(abs(item[0]-item[1]))
        ax[0].errorbar(replicaNo, thru, yerr=errs, fmt=style, mec=color, color=color, mfc='none', label='%s'%name, markersize=6)
#         ax[0].errorbar(replicaNo, thru, yerr=errs, marker='s', mfc='red', mec='green', ms=20, mew=4)
        ax[0].set_ylabel("Throughput (KTx/s)")
        ax[0].legend(loc='best', fancybox=True,frameon=False,framealpha=0.8)
        ax[0].set_xticks(xticks)
        ax[0].set_ylim([0,160])
        ax[0].set_xticklabels(xticks_label)
        ax[0].set_xticklabels(("", "", "", "", "", ""))
    lat = [
    ('HotStuff',[
        [8.5, 8.8],
        [18.4, 18.9],
        [52.2, 54.5],
        [206, 211],
    ], '-o', 'coral'),
    ('2C-HS',[
        [6.6, 6.7],
        [18.7, 19.1],
        [49.4, 50.5],
        [201, 206],
    ], '-^', 'darkseagreen'),
    ('Streamlet',[
        [6.4, 6.6],
        [18.15, 18.7],
        [47.3, 50.1],
        [193, 197],
    ], '-s', 'steelblue')
    ]
    for name, entries, style, color in lat:
        lat = []
        errs = []
        for item in entries:
            lat.append((item[0]+item[1])/2.0)
            errs.append(abs(item[0]-item[1]))
        ax[1].errorbar(replicaNo, lat, yerr=errs, fmt=style, color=color, mec=color, mfc='none', label='%s' % name, markersize=6)
        ax[1].set_ylabel("Latency (ms)")
        ax[1].set_xticks(replicaNo)
        ax[1].set_xticks(xticks)
        ax[1].set_ylim([0,220])
        ax[1].set_xticklabels(xticks_label)
    ax[0].grid(linestyle='--', alpha=0.3)
    ax[1].grid(linestyle='--', alpha=0.3)
    f.text(0.5, 0.04, 'Number of Nodes', ha='center', va='center')
    plt.subplots_adjust(hspace=0.1)
    plt.savefig('scalability.pdf', format='pdf')
    plt.show()

if __name__ == '__main__':
    do_plot()
