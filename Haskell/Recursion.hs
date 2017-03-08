-- Some maths functions...

import Hugs.Prelude

isPrime :: Int -> Bool
-- pre n >= 0
isPrime n
  = isPrime' n 3 
     where 
       isPrime' n m
         | n == 2            = True
         | n == 1 || n == 0  = False
         | m >= n            = True 
         | even n            = False
         | n `mod` m == 0    = False
         | otherwise         = isPrime' n (m+2)	

nextPrime :: Int -> Int
-- Returns the next prime number after n
nextPrime n
 | isPrime (n+1) == True  = n+1
 | otherwise              = nextPrime (n+1)


modPow :: Int -> Int -> Int -> Int
-- Calculates x^y mod n, uses exponention by squaring algorithm
modPow x y n
  | y == 0  = 1
  | y < 0   = square (1 `div` modPow x y n) n `mod` n
  | odd y   = x * square (modPow x ((y-1) `div` 2) n) n `mod` n
  | even y  = square (modPow x (y `div` 2) n) n `mod` n


square :: Int -> Int -> Int
-- Returns n^2 reduced by modulo m
square n m
 = (n * n `mod` m)

isCarmichael :: Int -> Bool
-- Determines if a given number is a Carmichael number
isCarmichael n
 | isPrime n == True = False
 | otherwise         = fermatTest (n-1) (n)
    where 
     fermatTest a n
       | a <= 0                        = True
       | modPow (a) (n) (n) /= a       = False
       | otherwise                     = fermatTest (a-1) (n)
   
nextSmithNumber :: Int -> Int
-- Returns the smallest Smith number > n
nextSmithNumber n
 | isPrime (n+1) == False && 
   sumList (primeFactors (n+1)) == sumDigits (n+1)  = n+1
 | otherwise                                        = nextSmithNumber (n+1) 

primeFactors :: Int -> [Int]
-- Returns a list of prime factors
primeFactors n
  = primeFactors' n 2
      where
        primeFactors' n m
          | m > n                        = []
          | isPrime m && n `mod` m == 0  = m : primeFactors' (n `div` m) (m)
          | otherwise                    
            = primeFactors' n (m + ((m `mod` 2) + 1))
 
sumList :: [Int] -> Int
-- Sums a given list of integers
sumList []        = 0
sumList (x : xs)  = sumDigits (x) + sumList (xs) 
 
sumDigits :: Int -> Int
-- Returns sum of digits in integer
sumDigits n
  | n `mod` 10 == n  = n
  | otherwise        = n `mod` 10 + sumDigits (n `div` 10)








