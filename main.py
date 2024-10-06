import sys
import pipeline.utils as utils
import pipeline.ingest as ingest
import pipeline.transform as transform

def _get_run_mode() -> str:
    print('Welcome to the weather monitor service')
    while True:
        run_mode = input('Would you like to:\n(ingest) new weather data\n(transform) and stage weather data\n(serve) weather data\n(exit)\n').lower()
        if run_mode == 'exit':
            exit
        elif run_mode != 'ingest' and run_mode != 'transform' and run_mode != 'serve':
            continue
        return run_mode

def _get_start_end_dates() -> tuple[str, str]:
    start_date = ''
    end_date = ''
    while True:
        data = input('Please enter a single date in iso format, or a start and end date seperated by a space: ')
        if data == 'exit':
            exit
        split_data = data.split(' ')
        if len(split_data) == 1:
            start_date = split_data[0]
            end_date = start_date
        elif len(split_data) == 2:
            start_date = split_data[0]
            end_date = split_data[1]
        else:
            print('Input must be two iso dates seperated by a space. Please try again')
            continue
        return (start_date, end_date)

def _print_mode(mode: str):
    print('-'*15 + '\n' + f'{mode.upper()} MODE' + '\n' + '-'*15)

def _handle_ingest_mode(run_type: bool = True):
    _print_mode('ingest')
    start_date = ''
    end_date = ''
   
    if not run_type and len(sys.argv) == 3:
        start_date = start_date = sys.argv[2]
        end_date = start_date
    elif not run_type and len(sys.argv) == 4:
        start_date = sys.argv[2]
        end_date = sys.argv[3]
    else:
        start_date, end_date = _get_start_end_dates()
        
    while not utils.valid_iso_date(start_date) or not utils.valid_iso_date(end_date):
        start_date, end_date = _get_start_end_dates()
    
    print(f'start date is: {start_date}\nend date is: {end_date}')
    
    ingest.run_ingest(start_date, end_date, verbose=True)

def _handle_transform_mode():
    _print_mode('transform')
    transform.run_transform(verbose=True)

def _handle_serve_mode():
    _print_mode('serve')
    print('Process not yet implemented')

def _handle_admin_mode():
    _print_mode('admin')
    if len(sys.argv) <= 2:
        return None
    
    match sys.argv[2]:
        case 'sync_raw_data':
            utils.download_raw_s3_to_local(verbose=True)
            utils.push_raw_local_to_s3(verbose=True)
        case 'download_s3_raw':
            utils.download_raw_s3_to_local(verbose=True)
        case 'push_local_raw':
            utils.push_raw_local_to_s3(verbose=True)
        case 'identify_missing_dates':
            try:
                print(utils.identify_missing_dates(sys.argv[3], sys.argv[4]))
            except IndexError:
                print('start and end dates must be defined iso format and seperated by a space')
        case 'get_s3_staging_files':
            print(utils.get_s3_staging_files(verbose=True))
        case 'push_staging_files':
            utils.push_staging_local_to_s3(verbose=True)

if __name__ == '__main__':
    """
    If there are system arguments present, run with those
    0th arg is the name of the file
    1st arg is the run mode [ingest, transform, serve]
    if run mode is ingest:
        2nd arg is the ingest start_date
        3rd arg is the ingest end_date
    """
    # Boolean to indicate command line (0) or terminal input (1)
    run_type = None
    # String to represent ingest, transform, or serve run modes
    run_mode = None
    if len(sys.argv) >= 2:
        run_type = False
        run_mode = sys.argv[1]
    else:
        run_type = True
        run_mode = _get_run_mode()
        
    if run_mode == 'ingest':
        _handle_ingest_mode(run_type)
    elif run_mode == 'transform':
        _handle_transform_mode()
    elif run_mode == 'serve':
        _handle_serve_mode()
    elif run_mode == 'admin':
        _handle_admin_mode()
