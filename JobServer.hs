{-# LANGUAGE ScopedTypeVariables #-}

--module JobServer (initializeJobServer, getJobServer, clearJobServer, runJobs, runJob, waitOnJobs,
--                  printJobServerHandle, JobServerHandle) where

import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, MVar, threadDelay, forkOS)
import Control.Exception.Base (assert)
import Control.Exception (catch, SomeException(..))
import Foreign.C.Types (CInt)
import System.Environment (getEnv, setEnv)
import System.Posix.IO (createPipe, fdWrite, fdRead, FdOption(..), setFdOption, closeFd)
import System.Posix.Types (Fd(..), ByteCount)
import System.IO (hPutStrLn, stderr)

newtype JobServerHandle a = JobServerHandle { unJobServerHandle :: (Fd, Fd, [MVar a]) }
newtype Token = Token { unToken :: String } deriving (Eq, Show)

initializeJobServer :: Int -> IO (JobServerHandle a)
initializeJobServer n = do 
  -- Create the pipe: 
  (readEnd, writeEnd) <- createPipe
  assert_ $ readEnd >= 0
  assert_ $ writeEnd >= 0
  assert_ $ readEnd /= writeEnd

  -- Make the read end of the pipe non-blocking:
  setFdOption readEnd NonBlockingRead True

  -- Write the tokens to the pipe:
  byteCount <- fdWrite writeEnd tokens
  assert_ $ countToInt byteCount == tokensToWrite

  -- Set an environment variable to store the handle for 
  -- other programs that might use this server:
  setEnv "MAKEFLAGS" $ show readEnd ++ ", " ++ show writeEnd
  
  -- Return the read and write ends of the pipe:
  return $ JobServerHandle (readEnd, writeEnd, [])
  where tokens = concat $ map show $ take tokensToWrite [(1::Integer)..]
        tokensToWrite = n-1

getJobServer :: IO (JobServerHandle a)
getJobServer = do flags <- getEnv "MAKEFLAGS"
                  let handle = handle' flags
                  return $ JobServerHandle $ (Fd $ handle !! 0, Fd $ handle !! 1, [])
  where handle' flags = map convert (splitBy ',' flags)
        convert a = read a :: CInt

clearJobServer :: JobServerHandle a -> IO ()
clearJobServer handle = safeCloseFd w >> safeCloseFd r
  where safeCloseFd fd = catch (closeFd fd) (\(_ :: SomeException) -> return ()) 
        (r, w, _) = unJobServerHandle handle

-- Given a list of IO () jobs, run them when a space on the job server is
-- available.
runJobs :: JobServerHandle a -> [IO a] -> IO [a]
runJobs _ [] = return []
runJobs _ [j] = do ret <- j 
                   return [ret]
runJobs handle (j:jobs) = maybe (j >> runJobs handle jobs) forkJob =<< getToken r
  where 
    (r, w, _) = unJobServerHandle handle
    forkJob token = do
      putStrLn $ "read " ++ unToken token ++ " from pipe. "

      -- Fork new thread to run job:
      mToken <- newEmptyMVar
      mReturn <- newEmptyMVar
      --putStrLn $ "fork process " ++ unToken token
      -- consider using fork finally
      -- consider putting thread id in handle so that it can be killed on error
      _ <- forkOS $ runForkedJob mToken mReturn w j
      putMVar mToken token

      -- Run the rest of the jobs:
      rets <- runJobs handle jobs

      -- Wait on my forked job:
      --putStrLn $ "waiting on " ++ unToken token
      ret1 <- takeMVar mReturn
      return $ ret1:rets 
      --putStrLn $ "reaped " ++ unToken returnedToken

runJob :: JobServerHandle a -> IO a -> IO (JobServerHandle a)
runJob handle j = maybe (j >> return handle) forkJob =<< getToken r
  where 
    (r, w, mReturns) = unJobServerHandle handle
    forkJob token = do
      putStrLn $ "read " ++ unToken token ++ " from pipe. "

      -- Fork new thread to run job:
      mToken <- newEmptyMVar
      mReturn <- newEmptyMVar
      --putStrLn $ "fork process " ++ unToken token
      -- consider using fork finally
      _ <- forkOS $ runForkedJob mToken mReturn w j
      putMVar mToken token
      return $ JobServerHandle (r, w, mReturns++[mReturn])

printJobServerHandle :: JobServerHandle a -> IO ()
printJobServerHandle handle = putStrLn $ "handle: (" ++ show r ++ ", " ++ show w ++ ", len " ++ show (length mvars) ++ ")"
  where (r, w, mvars) = unJobServerHandle handle

runForkedJob :: MVar (Token) -> MVar (a) -> Fd -> IO a -> IO ()
runForkedJob mToken mReturn w job = do 
  token <- takeMVar mToken
  --putStrLn $ "-- starting job with token: " ++ unToken token
  ret <- job
  --putStrLn $ "-- finished job with token: " ++ unToken token

  -- Return the token:
  returnToken w token
   
  -- Signal that I have finished:
  putMVar mReturn ret
  return ()

-- todo return status here
waitOnJobs :: JobServerHandle a -> IO [a]
waitOnJobs handle = mapM takeMVar mReturns
  where
    (_, _, mReturns) = unJobServerHandle handle

-- Get a token if one is available, otherwise return Nothing:
getToken :: Fd -> IO (Maybe Token)
getToken fd = catch (readPipe) (\(_ :: SomeException) -> return Nothing) 
  where readPipe = do (token, byteCount) <- fdRead fd 1
                      assert_ $ countToInt byteCount == 1
                      return $ Just $ Token $ token

-- Return a token to the pipe:
returnToken :: Fd -> Token -> IO ()
returnToken fd token = do byteCount <- fdWrite fd (unToken token)
                          assert_ $ countToInt byteCount == 1

-- Convenient assert function:
assert_ :: Monad m => Bool -> m ()
assert_ c = assert c (return ())

-- Conversion helper for ByteCount type:
countToInt :: ByteCount -> Int
countToInt a = fromIntegral a

splitBy :: Char -> String -> [String]
splitBy delimiter = foldr f [[]] 
  where f c l@(x:xs) | c == delimiter = []:l
                     | otherwise = (c:x):xs
        f _ [] = []

-- Main function:
main :: IO ()
main = do handle <- initializeJobServer 4
          printJobServerHandle handle
          returns <- runJobs handle jobs
          putStrLn $ "returns: " ++ show returns
          putStrLn "--------------------------------------------------------------------"
          handle2 <- getJobServer
          printJobServerHandle handle2
          handle3 <- mapM' runJob handle2 jobs
          printJobServerHandle handle3
          returns2 <- waitOnJobs handle3
          putStrLn $ "returns: " ++ show returns2
          clearJobServer handle
  where jobs = [exampleLongJob "A", exampleJob "B", exampleLongJob "C", 
                exampleJob "D", exampleJob "E", exampleJob "F",
                exampleJob "G", exampleJob "H", exampleJob "I",
                exampleJob "J", exampleJob "K", exampleJob "L"]
        mapM' :: Monad m => (a -> m b -> m a) -> a -> [m b] -> m a
        mapM' _ a [] = return a
        mapM' f a (x:xs) = do newA <- f a x
                              mapM' f newA xs

exampleJob :: String -> IO (Int)
exampleJob n = do putStrLn $ ".... Running job: " ++ n
                  threadDelay 1000000
                  putStrLn $ ".... Finishing job: " ++ n
                  return 1

exampleLongJob :: String -> IO (Int)
exampleLongJob n = do putStrLn $ ".... Running job: " ++ n
                      threadDelay 10000000
                      putStrLn $ ".... Finishing job: " ++ n
                      return 2


