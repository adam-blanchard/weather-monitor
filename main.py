import pipeline.ingest as ingest
import pipeline.transform as transform
import pipeline.other as other

if __name__ == '__main__':
    print('Welcome to the weather monitor service')
    run_mode = None
    while True:
        run_mode = input('Would you like to:\n1 - ingest new weather data\n2 - transform and stage weather data\n3 - serve weather data\n4 - other\n')
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
        ingest.run_ingest()
    elif run_mode == 2:
        print('-'*15 + '\n' + 'TRANSFORM MODE' + '\n' + '-'*15)
        transform.run_transform()
    elif run_mode == 3:
        print('-'*15 + '\n' + 'SERVE MODE' + '\n' + '-'*15)
        print('Process not yet implemented')
    elif run_mode == 4:
        print('-'*15 + '\n' + 'OTHER MODE' + '\n' + '-'*15)
        other.run_other()
