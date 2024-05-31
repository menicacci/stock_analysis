

def round_val(number):
    return round(number, 2)


def calculate_percentual_variation(first_close, last_close):
    return round_val(((last_close - first_close) / first_close) * 100)


def flat_keys_values(keys_values_array):
    return keys_values_array.map(
        lambda elem: (*elem[0], *elem[1])
    )


def array_to_csv(array):
    return array.map(lambda row: ','.join(map(str, row)))
