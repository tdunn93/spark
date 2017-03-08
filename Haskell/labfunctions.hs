--UES 2: Functions

addDigit :: Int -> Int -> Int
-- Adds single digit to end of integer
addDigit x y
  = x + y

convert :: Float -> Float
-- converts temperature in degrees to farenheight
convert t
  = (t * 9/5) + 32  

type Vertex = (Float, Float)

distance :: Vertex -> Vertex -> Float 
-- Calculates distance between two points
distance (a, b) (c, d) 
  = sqrt((a-c)^2 + (b-d)^2)

triArea :: Vertex -> Vertex -> Vertex -> Float
-- Calculates area of a triangle given 3 vertices
triArea (x1, y1) (x2, y2) (x3, y3)
  = sqrt(s*(s-a)*(s-b)*(s-c))
  where
    a = distance (x1, y1) (x2, y2)
    b = distance (x1, y1) (x3, y3)
    c = distance (x2, y2) (x3, y3)
    s = (a + b + c)/2

fact :: Int -> Int
-- Pre: n >= 0
fact n
  | n == 0  = 1
  | otherwise  = n * fact (n-1)


remainder :: Int -> Int -> Int
-- Caclulates reminader after integer division
remainder n d
  | n == d  = 0
  | n < d  = n 
  | otherwise  = remainder (n-d) d

fib :: Int -> Int
-- Calculates nth fib number
fib n
  | n == 0 || n == 1   = 1
  | otherwise  = fib(n-1) + fib(n-2)

