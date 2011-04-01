all: ebin compile

ebin:
	mkdir -p ebin

clean:
	rm -rf ebin

compile:
	erl -pa build -noinput +B -eval 'case make:all([{i, "'${COUCH_SRC}'"}]) of up_to_date -> halt(0); error -> halt(1) end.'
