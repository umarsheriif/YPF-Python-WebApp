from flask import Flask
import numpy as np
import io
import pandas as pd
import os, uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess

app = Flask(__name__)


block_blob_service = BlockBlobService(account_name='ypfdatastore', account_key='5ouM2u3uMv/uPOUxv6Izs9gJgwr+4vjIuBD8I6mFaIrvbJJz564/rJ8q1mI5BiCqykPTiKtfL6M8uy2cCN0aAg==')

container_name ='ypfcsvs'
blobname = 'epf15_before.csv'

local_path=os.path.expanduser("/Files")




def main(filename_meas='/Files/epf15_before.csv'):  


    # read the measurement csv file
    x = pd.read_csv(filename_meas, header=None, names=('tag_name','date_time','value_float'))
    time_start = x['date_time'].min()
    time_end = x['date_time'].max()

    # create np array to write the uniformly sampled data
    new_index = pd.date_range(start=time_start, end = time_end, freq='30S')
    all_meas_unif = np.zeros((new_index.size,18))

    # resample all 18 sensor measurements
    all_meas_unif[:,0] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-001A.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,1] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-002K.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,2] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-050A.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,3] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-050B.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,4] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-406.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,5] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-501A.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,6] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-501B.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,7] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-501C.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,8] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-501D.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,9] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-501F.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,10] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-507.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,11] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-507A.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,12] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-601.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,13] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_PT-604.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,14] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_FT-405.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,15] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_COL_FT-506.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,16] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_LT-501A.PV'], new_index.size, time_start, time_end)
    all_meas_unif[:,17] = select_sensor_meas(x.loc[x.tag_name == 'LLN_EPF15_LT-501B.PV'], new_index.size, time_start, time_end)
    
    local_file_name ="EPF_15" + ".csv"

    full_path_to_file =os.path.join(local_path, local_file_name)

    with open(full_path_to_file, 'wb') as abc:
        np.savetxt(abc, all_meas_unif, delimiter=",")
    block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)
def select_sensor_meas(sensor_meas, Nlen, time_start, time_end):
    if sensor_meas.size > 0:
        sensor_meas_series = pd.Series(sensor_meas.iloc[:,2])
        sensor_meas_series.index = pd.to_datetime(sensor_meas.iloc[:,1])
        sensor_meas_unif = uniformly_sampled_meas(sensor_meas_series, time_start, time_end)
    else:
        sensor_meas_unif = np.zeros((Nlen,))
    return sensor_meas_unif


def uniformly_sampled_meas(asynch_meas, time_start, time_end):

    new_index = pd.date_range(start=time_start, end = time_end, freq='30S')
    new_series = pd.Series(np.nan, index=new_index)

    # concat the old and new series and remove duplicates (if any) 
    comb_series = pd.concat([asynch_meas, new_series])
    comb_series = comb_series[~comb_series.index.duplicated(keep='first')]

    # interpolate to fill the NaNs
    comb_series.interpolate(method='time', inplace=True)

    # resampled data
    resamp_meas = comb_series[new_index]
    return resamp_meas.values

@app.route("/")
def hello():
    block_blob_service.get_blob_to_path(container_name,blobname,local_path)
    main()
    return "Uniform Sampling is getting ready!"

