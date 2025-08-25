"""
Just a class to help with data vis
"""

import matplotlib.pyplot as plt

class VisHandler():

    def __init__(self):
        pass

    def createShowLineGraph(self, x_axis, y_axis : list, options : dict):
        fig, ax = plt.subplots(fixsize=(10, 6))
        for data in y_axis:
            ax.plot(x_axis, data)
        if options['x_label']: ax.set_xlabel(options['x_label'])
        if options['y_label']: ax.set_ylabel(options['y_label'])
        if options['title']: ax.set_title(options['title'])
        if options['legend']: ax.legend()
        if options['date_format']: fig.autofmt_xdate()
        plt.show()