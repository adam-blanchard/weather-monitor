if __name__ == '__main__':
    my_prefix = 'openweather_dailyagg_'
    my_str = 'openweather_dailyagg_2024-01-01.json'
    print(my_str.removeprefix(my_prefix).removesuffix('.json'))