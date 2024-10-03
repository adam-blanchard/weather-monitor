import pipeline.utils as utils
import pipeline.ingest as ingest
import pipeline.transform as transform

if __name__ == '__main__':
    print('Welcome to the weather monitor service')
    run_mode = None
    while True:
        run_mode = input('Would you like to:\n1 - ingest new weather data\n2 - transform and stage weather data\n3 - serve weather data\n')
        try:
            run_mode = int(run_mode)
        except TypeError:
            print('Input is not a number. Please try again')
            continue
        if run_mode != 1 and run_mode != 2 and run_mode != 3 and run_mode != 4:
            continue
        break
        
    if run_mode == 1:
        print('-'*15 + '\n' + 'INGEST MODE' + '\n' + '-'*15)
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
            print(f'Your chosen dates are\nstart date: {start_date}\nend date: {end_date}')
            input_again = input('Are you happy with these dates? (y/n) ')
            if input_again != 'y':
                continue
            proceed = input('Would you like to fetch all data within this range? (y/n) ')
            if proceed != 'y':
                exit
            break
        ingest.run_ingest(start_date, end_date)
    elif run_mode == 2:
        print('-'*15 + '\n' + 'TRANSFORM MODE' + '\n' + '-'*15)
        transform.run_transform()
    elif run_mode == 3:
        print('-'*15 + '\n' + 'SERVE MODE' + '\n' + '-'*15)
        print('Process not yet implemented')
