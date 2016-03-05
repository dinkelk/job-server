# Dependencies:
source=`ls *.hs`
redo-ifchange $source

# Compile redo.hs to filename $3 with verbosity set to low 
# so there is nothing printed to standard out
ghc -O2 -v0 -Wall -threaded -o $3 JobServer.hs 1>&2
