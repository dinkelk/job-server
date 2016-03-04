{-# LANGUAGE ForeignFunctionInterface #-}
import Control.Concurrent (newEmptyMVar, putMVar, takeMVar, MVar, threadDelay, forkIO)
import Control.Exception.Base (assert)
import System.Posix.IO (createPipe, fdWrite, fdRead, FdOption(..), setFdOption)
import System.Posix.Types (Fd, ByteCount)

-- Main function:
main :: IO ()
main = do (readEnd, writeEnd) <- initServer 5
          putStrLn $ "created pipe with fd: (" ++ show readEnd ++ ", " ++ show writeEnd ++ ")"
          doWork readEnd writeEnd
  where doWork r w = do
          (token, byteCount) <- fdRead r 1
          assert_ $ countToInt byteCount == 1
          putStrLn $ "read " ++ token ++ " from pipe. "
          m <- newEmptyMVar
          -- consider using fork finally
          threadId <- forkIO $ someWork m
          putStrLn $ "fork process " ++ token
          putMVar m token
          putStrLn $ "waiting on process " ++ token
          returnedToken <- takeMVar m
          putStrLn $ "reaped " ++ returnedToken
          doWork r w

assert_ :: Monad m => Bool -> m ()
assert_ c = assert c (return ())

someWork :: MVar (String) -> IO ()
someWork m = do token <- takeMVar m
                putStrLn $ "... working on token: " ++ token
                threadDelay 1000000
                putStrLn $ "... finished token: " ++ token
                putMVar m token
                return ()

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
