{-# LANGUAGE ScopedTypeVariables #-}
import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, MVar, threadDelay, forkIO)
import Control.Exception.Base (assert)
import Control.Exception (catch, SomeException(..))
import System.Posix.IO (createPipe, fdWrite, fdRead, FdOption(..), setFdOption)
import System.Posix.Types (Fd, ByteCount)

newtype Token = Token { unToken :: String } deriving (Eq, Show)

-- Main function:
main :: IO ()
main = do (readEnd, writeEnd) <- initServer 5
          putStrLn $ "created pipe with fd: (" ++ show readEnd ++ ", " ++ show writeEnd ++ ")"
          runJobs readEnd writeEnd

runJobs :: Fd -> Fd -> IO ()
runJobs r w = maybe (exampleJob 3) forkJob =<< getToken r
  where forkJob token = do
          putStrLn $ "read " ++ unToken token ++ " from pipe. "
          m <- newEmptyMVar
          -- consider using fork finally
          threadId <- forkIO $ runJob m (exampleJob 2)
          putStrLn $ "fork process " ++ unToken token
          putMVar m token
          --exampleJob 1
          putStrLn $ "waiting on " ++ unToken token
          returnedToken <- takeMVar m
          putStrLn $ "reaped " ++ unToken returnedToken
          returnToken w returnedToken
          runJobs r w

runJob :: MVar (Token) -> IO () -> IO ()
runJob m job = do token <- takeMVar m
                  putStrLn $ "-- starting job with token: " ++ unToken token
                  job
                  putStrLn $ "-- finished job with token: " ++ unToken token
                  putMVar m token
                  return ()

exampleJob :: Int -> IO ()
exampleJob n = do putStrLn $ ".... Running job: " ++ show n
                  threadDelay 1000000
                  putStrLn $ ".... Finishing job: " ++ show n
                

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

assert_ :: Monad m => Bool -> m ()
assert_ c = assert c (return ())


initServer :: Int -> IO (Fd, Fd)
initServer n = do -- Create the pipe: 
                  (readEnd, writeEnd) <- createPipe
                  assert_ $ readEnd >= 0
                  assert_ $ writeEnd >= 0
                  assert_ $ readEnd /= writeEnd

                  -- Make the read end of the pipe non-blocking:
                  setFdOption readEnd NonBlockingRead True

                  -- Write the tokens to the pipe:
                  byteCount <- fdWrite writeEnd tokens
                  assert_ $ countToInt byteCount == tokensToWrite
                  
                  -- Return the read and write ends of the pipe:
                  return (readEnd, writeEnd)
  where tokens = concat $ map show $ take tokensToWrite [(1::Integer)..]
        tokensToWrite = n-1


countToInt :: ByteCount -> Int
countToInt a = fromIntegral a
