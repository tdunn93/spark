-- Haskell HTML Parser
-- Builds HTML file from a given template and info file
-- Example file builds a sample artist page for the duo 4hero
-----------------------------------------------------------------------------
--
-- The I/O is given in function main
--
--
--
main :: String -> String -> IO()
main tempfile infofile 
  = do t <- readFile ( tempfile ++ ".html" )
       i <- readFile ( infofile ++ ".info" )
       writeFile ( infofile ++ ".html" ) ( buildHtml t i )


type FileContents = String

type Keyword      = String
type KeywordValue = [ String ]
type KeywordDefs  = [ ( Keyword, KeywordValue ) ]
--
-- A useful constant...
--
keywords :: [ Keyword ]
keywords
  = [ ":NAME", ":EMAIL", ":DISCOGS", ":BACKGROUND", ":RELEASE", ":DESC"]

--
-- The following function splits a given list of words into two
-- sublists: 1. those words up to, but not including, 
-- the first given `keyword';
-- 2. the remaining words in the input, including the keyword.
-- Any number of special keywords can be specified - these are supplied
-- as an additional parameter of type [ String ].
--
breakAt :: [ String ] -> [ String ] -> ( [ String ], [ String ] )
breakAt [] keywords
  = ( [], [] )
breakAt ( x : xs ) keywords
  | elem x keywords = ( [], x : xs )
  | otherwise = ( x : before, after )
  where
      ( before, after ) = breakAt xs keywords

-----------------------------------------------------------------------------

lookUp :: String -> [(String, a)] -> [a]
-- Returns the list of items whose string matches the search string
lookUp str l1 
   = [snd i | i <-l1, fst(i) == str]

getKeywordDefs :: [String] -> KeywordDefs
-- Returns a list of keyword/value pairs
getKeywordDefs str
  | str == []              = [] 
  | isKeyword (head str)   = (head str, getList (tail str)) : getKeywordDefs (tail str) 
  | otherwise              = getKeywordDefs (tail str)

getList :: [String] -> [String]
-- Returns list of keyword values
getList str
  | str == [] || isKeyword (head str)  = []
  | otherwise                          = head str : getList (tail str)

isKeyword :: String -> Bool
-- Returns true if given string is a keyword
isKeyword str
  | [x | x <- keywords, x == str] == []  = False
  | otherwise                            = True

buildProjects :: KeywordDefs -> String
-- Returns a string representing the HTML list items
buildProjects l1
  | l1 == []                      = []
  | fst (head l1) == ":RELEASE"   = "<p><li><i>" ++ unwords (getList (snd (head l1))) ++ "</i>" ++ buildProjects (tail l1)
  | fst (head l1) == ":DESC"      = "<p>" ++ unwords (getList (snd (head l1))) ++ "</li>" ++ buildProjects (tail l1)
  | otherwise                     = buildProjects (tail l1)

buildKeywords :: String -> KeywordDefs -> String
-- Returns a HTML string representing the relevant keyword
buildKeywords str l1
  | l1 == []              = []
  | fst (head l1) == str  = unwords (getList (snd (head l1)))
  | otherwise             = buildKeywords (str) (tail l1)

buildHtml :: FileContents -> FileContents -> FileContents
-- Takes contents of template and information files and returns a string of HTML output
buildHtml template info
  = unwords (buildHtml' (words (template)) (words (info)))
      where 
        buildHtml' :: [String] -> [String] -> [String]
        buildHtml' str1 str2
          | str1 == []                    = []
          | head (str1) == ":RELEASELIST" = buildProjects (getKeywordDefs str2) : buildHtml' (tail str1) (str2) 
          | isKeyword (head str1)         = buildKeywords (head str1) (getKeywordDefs str2) : buildHtml' (tail str1) (str2)
          | otherwise                     = head (str1) : buildHtml' (tail str1) (str2)
           
