-- short module written to aid in cracking some ciphertext by freq analysis
-- usage:
-- decrypt Pairs cipherlist
-- Pairs: pair the ciphertext with frequency alphabet
-- e.g pairs = pairElem (take 26 . reverse . sortBy (compare `on` snd) . g $ cipherlist) alpha
-- where alpha = alphabet to use

module Cipher where
import Data.List
import Data.Function

cipherlist = splitOn' (==' ') "241 26 73 21 36 9 201 248 76 23 28 79 10 41 82 21 22 81 18 36 75 28 213 67 24 39 253 28 36 73 31 30 75 16 213 81 17 26 253 240 22 74 11 30 81 201 24 69 10 33 73 14 35 68 14 227 253 249 33 66 10 40 66 201 40 66 23 25 253 34 36 82 27 213 80 24 33 82 29 30 76 23 213 62 23 25 253 236 11 253 29 36 253 18 24 62 23 24 76 13 26 29 16 22 74 11 30 81 27 26 80 14 22 79 12 29 11 12 36 74 201 38 82 24 41 70 23 28 253 27 26 67 14 39 66 23 24 66 227 213 20 15 25 18 12 229 17 222 23 66 215"
pairs = reverse . sortBy (compare `on` snd) . g $ cipherlist

--splitOn taken from hackage
splitOn' :: (a -> Bool) -> [a] -> [[a]]
splitOn' f xs = split xs
  where split xs = case break f xs of
                      (chunk,[])     -> chunk : []
                      (chunk,_:rest) -> chunk : split rest

-- | split at regular intervals
chunk :: Int -> [a] -> [[a]]
chunk _ [] = []
chunk n xs = y1 : chunk n y2
  where
      (y1, y2) = splitAt n xs

-- standard grouping for frequencie of letters
g = map (\x -> (head x, length x )) . group . sort 

pairElem :: [(String, Int)] -> [String] -> [(String, String)]
pairElem [] [] = []
pairElem (x : xs) (y : ys)
  | (fst x) == "79" = zip (words . fst $ x) (words "E") ++ pairElem xs ys
  | (fst x) == "34" = zip (words . fst $ x) (words "R") ++ pairElem xs ys
  -- if the freq is less than 4 then just sub original cipherletter
  | (snd x) < 0  = zip (words . fst $ x) (words $ (fst $ x) ++ "") ++ pairElem xs ys
  | otherwise  = zip (words . fst $ x) (words y) ++ pairElem xs ys

lookUp :: [(String, String)] -> String -> String
lookUp [] _ = ""
lookUp (x : xs) s
  | fst x == s  = (snd (x))
  | otherwise   = lookUp xs s

decrypt :: [(String, String)] -> [String] -> String
decrypt _ [] = []
decrypt keyPairs (x : xs) = lookUp keyPairs x ++ decrypt keyPairs xs 

-- decrypt Pairs cipherlist
-- pairs = pairElem (take 26 . reverse . sortBy (compare `on` snd) . g $ cipherlist) alpha
-- alpha = alphabet to use

ld :: String -> String -> Int
ld [] xs = length xs
ld ys [] = length ys
ld (x : xs) (y : ys)
  | x == y      = 0 + ld (xs) (ys)
  |otherwise    = 1 + min (ld (x : xs) (ys)) (min (ld xs ys) (ld xs (y : ys)))

-- some standard functions to see if i still remember this stuff...!
map' :: (a -> b) -> [a] -> [b]
map' f []   = []
map' f (x : xs) = ([f x]) ++ (map' f xs)

foldr' :: (a -> b -> b) -> b -> [a] -> b
foldr' f i [] = i
foldr' f i (x : xs) = f x (foldr' f i xs)

rev :: String -> String
rev [] = []
rev (x : [])  = [x]
rev (x : xs)  = (rev (xs)) ++ [x]
