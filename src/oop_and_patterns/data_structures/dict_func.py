from typing import List, Optional

# Define a series of functions that perform basic arithmetic operations
def first_function(a: int, b: int) -> int:
    return a + b

def second_function(a: int, b: int) -> int:
    return a - b

def third_function(a: int, b: int) -> int:
    return a * b

def fourth_function(a: int, b: int) -> int:
    return a / b

def fifth_function(a: int, b: int) -> int:
    return a % b

def sixth_function(a: int, b: int) -> int:
    return a ** b

# Default function to use if no valid key is provided
def default_function(a: int, b: int) -> int:
    return a + b

if __name__ == "__main__":
    import sys
    # Dictionary mapping integers to the corresponding functions
    funcs: dict = {
        0: first_function,
        1: second_function,
        2: third_function,
        3: fourth_function,
        4: fifth_function,
        5: sixth_function
    }

    # Get the input argument from the command line
    input_args = int(sys.argv[1])

    # Retrieve the function from the dictionary, defaulting to default_function if the key is not found
    final = funcs.get(input_args, default_function)
    
    # Example values for the function arguments
    a = 1
    b = 2
    
    # Call the selected function with the example arguments
    res = final(a, b)
    
    print(input_args, type(input_args), final.__name__)

    # Print the result
    print(res)