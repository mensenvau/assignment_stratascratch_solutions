import sys

def read_grid(): 
    data = sys.stdin.read().splitlines() 
    m, n = map(int, data[0].split())
    grid = data[1:]
    return m, n, grid
 
def create_prefix_sum(grid, n, m):
    prefix = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(n):
        for j in range(m):
            prefix[i + 1][j + 1] = int(grid[i][j]) + prefix[i + 1][j] + prefix[i][j + 1] - prefix[i][j]
    return prefix

def is_square(x, y, size, prefix):
    # Calculate sum of each segment of the square and compare to 'size'
    if (prefix[x + 1][y + size] - prefix[x][y + size] - prefix[x + 1][y] + prefix[x][y] != size):
        return False 
    if (prefix[x + size][y + size] - prefix[x + size - 1][y + size] - prefix[x + size][y] + prefix[x + size - 1][y] != size):
        return False 
    if (prefix[x + size][y + 1] - prefix[x + size][y] - prefix[x][y + 1] + prefix[x][y] != size):
        return False
    if (prefix[x + size][y + size] - prefix[x + size][y + size - 1] - prefix[x][y + size] + prefix[x][y + size - 1] != size):
        return False
    return True

def is_cross(grid, x, y, size):
    for i in range(size):
        if grid[x + i][y + i] != "1" or grid[x + i][y + size - 1 - i] != "1":
            return False
    return True

def find_square(grid, n, m, prefix):
    for i in range(n - 4): 
        for j in range(m - 4):
            if grid[i][j] == "1": 
                for size in range(5, min(n - i, m - j) + 1):  
                    if is_square(i, j, size, prefix):  
                        if is_cross(grid, i, j, size): 
                            return "Marked"
                        else:
                            return "Not marked"
    return "Printing error"

m, n, grid = read_grid()
prefix = create_prefix_sum(grid, n, m)
result = find_square(grid, n, m, prefix)

sys.stdout.write(result)