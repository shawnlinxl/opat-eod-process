def progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """Call in a loop to create terminal progress bar
    
    Arguments:
        iteration {int} -- current iteration
        total {int} -- total iterations
    
    Keyword Arguments:
        prefix {str} -- prefix string (default: {''})
        suffix {str} -- suffix string (default: {''})
        decimals {int} -- positive number of decimals in percent complete (default: {1})
        length {int} -- character length of bar (default: {100})
        fill {str} -- bar fill character (default: {'█'})
    """

    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()
