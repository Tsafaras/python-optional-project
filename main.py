import matplotlib.pyplot as plt
from pymongo.database import Database
from requests import get
from gzip import open as unz
from shutil import copyfileobj
from os import remove
from pandas import read_csv, to_numeric, DataFrame
from pymongo import MongoClient


def yes_or_no(question: str):
    while True:
        reply = input(question + ' (y/n): ').lower().strip()
        if reply[:1] == 'y':
            return True
        elif reply[:1] == 'n':
            return False


def getData(url: str, file: str):
    # download the file
    r = get(url, allow_redirects=True)
    open(file, 'wb').write(r.content)

    # unzip file
    with unz(file, 'rb') as f_in:
        with open(file[:len(file) - 3], 'wb') as f_out:
            copyfileobj(f_in, f_out)

    # delete zipped file (no longer needed)
    remove(file)


def readData(file: str):
    # Only 1st columns contains a 'c', and we want years 2017-2020
    important = 'c|2017|2018|2019|2020'
    df = read_csv(file, delimiter='\t')

    # drop columns whose header doesn't contain words in (important) string
    df = df.loc[:, df.columns.str.contains(important)]

    # get rows whose 1st column has EL (for Greece) or AT (for Austria) (at last 2 chars of its value)
    EL = df[df[df.columns[0]].str[-2:] == 'EL']
    AT = df[df[df.columns[0]].str[-2:] == 'AT']
    # combine them into one DataFrame
    df = EL.append(AT)

    # drop rows that contains PCH (percentage change) in 1st column
    df = df[~df[df.columns[0]].str.contains('PCH|TOTAL')]
    # remove spaces in column headers
    df.columns = df.columns.str.replace(' ', '')
    # make headers of dates appear better
    df.columns = df.columns.str.replace('M', '-')

    # replace values starting with ':', with 0
    df.replace('^:', 0, inplace=True, regex=True)
    # remove values that contain spaces OR spaces followed by a letter
    # OR remove pattern so as to leave 'NAT|FOR,AT|EL' at 1st column
    df.replace(' [a-zA-Z]| |,NR.+?(?=,)', '', inplace=True, regex=True)

    # all columns except the 1st one
    cols = df.columns[1:]
    # change cols columns data type to numeric
    df[cols] = df[cols].apply(to_numeric)
    # group by 1st column and add the rest
    df = df.groupby(df[df.columns[0]])[cols].sum().reset_index()
    # reset_index() because groupby returns groupby object, but we want a DataFrame

    # change header of 1st column, since some values have been removed
    df.columns.values[0] = 'c_resid,country'
    return df


def storeData(df: DataFrame, collection: str, connection: Database):
    # Store the dataframe into a csv file, named '(collection).csv'
    df.to_csv(collection + '.csv', index=False)
    # Store the dataframe into MongoDB collection named 'collection'
    # Must be transformed to dictionary (among others) to be accepted
    connection[collection].insert_many(df.to_dict('records'))


def split_Data(df: DataFrame, title: str):
    # Make 1st column as index
    df.set_index(df.columns[0], inplace=True)
    # Reverse all the rows, so as to have dates in ascending order
    df = df[df.columns[::-1]]
    # Use 1st (transposed) column
    df1 = df.iloc[0, :].transpose()
    # Use 2nd (transposed) column
    df2 = df.iloc[1, :].transpose()
    # Make a plot with these two
    make_plots(df1, df2, title + ' by non-residents')
    # Use 3rd (transposed) column
    df1 = df.iloc[2, :].transpose()
    # Use 4th (transposed) column
    df2 = df.iloc[3, :].transpose()
    # Make a plot with these two
    make_plots(df1, df2, title)


def make_plots(df1: DataFrame, df2: DataFrame, title: str):
    # Prepare the plot
    fig, ax = plt.subplots()
    # Set its title
    fig.suptitle(title + ' at tourist accomodation establishments')
    # Make 1st subplot bar, enabling legend and setting color
    df1.plot.bar(ax=ax, legend=True, color=(0.8, 0.2, 0.5, 0.8), rot=45)
    # Make 2nd subplot bar, enabling legend and setting color
    df2.plot.bar(ax=ax, legend=True, color=(0.3, 0.4, 0.8, 0.6), rot=45)
    # Show every 6th label, starting from 1st one
    for i, t in enumerate(ax.get_xticklabels()):
        if (i % 6) != 0:
            t.set_visible(False)
    # make room for x_axis lables
    plt.tight_layout()
    # Save the plot, show it and then close it so as to make room for next ones
    plt.savefig(title)
    plt.show()
    plt.close()


if yes_or_no("Do you want to download the files from EUROSTAT website?"):
    getData(
        'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'
        'tour_occ_nim.tsv.gz', 'nights.tsv.gz')
    getData('https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/'
            'tour_occ_arm.tsv.gz', 'arrivals.tsv.gz')

nights = readData('nights.tsv')
arrivals = readData('arrivals.tsv')

if yes_or_no("Would you like to store the data?"):
    client = MongoClient('localhost', 27017)
    client.drop_database('PythonOptional')
    db = client['PythonOptional']
    storeData(nights, 'Nights', db)
    storeData(arrivals, 'Arrivals', db)
    client.close()

split_Data(nights, 'Nights spent')
split_Data(arrivals, 'Arrivals')
