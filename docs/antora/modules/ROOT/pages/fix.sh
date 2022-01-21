for name in *.adoc
do 
 asciidoc-coalescer.rb  $name > ../../../antora/modules/ROOT/pages/$name
done