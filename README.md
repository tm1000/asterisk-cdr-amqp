# Asterisk CEL AMQP

This was originally taken from this reviewboard https://reviewboard.asterisk.org/r/4365/

Make sure you have the Asterisk header files along with the header files from https://github.com/wazo-pbx/wazo-res-amqp

To install

    apt-get install librabbitmq-dev
    make
    make install
    make samples

Configure the file in /etc/asterisk/cel_amqp.conf

You need to have res_amqp.so loaded.

Please restart asterisk before loading cel_amqp.so for the documentation.

To load module

    CLI> module load cel_amqp.so

There is a amqp command on the CLI to get the status.