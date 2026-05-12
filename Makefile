.PHONY: deb clean-deb

deb:
	./packaging/build-deb.sh

clean-deb:
	rm -rf target/debian
