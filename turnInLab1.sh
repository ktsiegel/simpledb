git commit -a -m 'Lab 1'
git tag -d lab1
git push origin :refs/tags/lab1
git tag -a lab1 -m 'lab1'
git push origin master
git push origin lab1
