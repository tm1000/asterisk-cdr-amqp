/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2015, Digium, Inc.
 *
 * David M. Lee, II <dlee@digium.com>
 *
 * See http://www.asterisk.org for more information about
 * the Asterisk project. Please do not directly contact
 * any of the maintainers of this project for assistance;
 * the project provides a web site, mailing lists and IRC
 * channels for your use.
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */

/*! \file
 *
 * \brief AMQP CDR Backend
 *
 * \author David M. Lee, II <dlee@digium.com>
 */

/*** MODULEINFO
	<depend>res_amqp</depend>
	<support_level>core</support_level>
 ***/

/*** DOCUMENTATION
	<configInfo name="cdr_amqp" language="en_US">
		<synopsis>AMQP CDR Backend</synopsis>
		<configFile name="cdr_amqp.conf">
			<configObject name="global">
				<synopsis>Global configuration settings</synopsis>
				<configOption name="loguniqueid">
					<synopsis>Determines whether to log the uniqueid for calls</synopsis>
					<description>
						<para>Default is no.</para>
					</description>
				</configOption>
				<configOption name="loguserfield">
					<synopsis>Determines whether to log the user field for calls</synopsis>
					<description>
						<para>Default is no.</para>
					</description>
				</configOption>
				<configOption name="connection">
					<synopsis>Name of the connection from amqp.conf to use</synopsis>
					<description>
						<para>Specifies the name of the connection from amqp.conf to use</para>
					</description>
				</configOption>
				<configOption name="queue">
					<synopsis>Name of the queue to post to</synopsis>
					<description>
						<para>Defaults to asterisk_cdr</para>
					</description>
				</configOption>
				<configOption name="exchange">
					<synopsis>Name of the exchange to post to</synopsis>
					<description>
						<para>Defaults to empty string</para>
					</description>
				</configOption>
			</configObject>
		</configFile>
	</configInfo>
 ***/

#include "asterisk.h"

#include "asterisk/cdr.h"
#include "asterisk/config_options.h"
#include "asterisk/json.h"
#include "asterisk/module.h"
#include "asterisk/amqp.h"
#include "asterisk/stringfields.h"

#define CDR_NAME "AMQP"
#define CONF_FILENAME "cdr_amqp.conf"

/*! \brief global config structure */
struct cdr_amqp_global_conf {
	AST_DECLARE_STRING_FIELDS(
		/*! \brief connection name */
		AST_STRING_FIELD(connection);
		/*! \brief queue name */
		AST_STRING_FIELD(queue);
		/*! \brief exchange name */
		AST_STRING_FIELD(exchange);
	);
	/*! \brief whether to log the unique id */
	int loguniqueid;
	/*! \brief whether to log the user field */
	int loguserfield;

	/*! \brief current connection to amqp */
	struct ast_amqp_connection *amqp;
};

/*! \brief cdr_amqp configuration */
struct cdr_amqp_conf {
	struct cdr_amqp_global_conf *global;
};

/*! \brief Locking container for safe configuration access. */
static AO2_GLOBAL_OBJ_STATIC(confs);

static struct aco_type global_option = {
	.type = ACO_GLOBAL,
	.name = "global",
	.item_offset = offsetof(struct cdr_amqp_conf, global),
	.category = "^global$",
	.category_match = ACO_WHITELIST,
};

static struct aco_type *global_options[] = ACO_TYPES(&global_option);

static void conf_global_dtor(void *obj)
{
	struct cdr_amqp_global_conf *global = obj;
	ao2_cleanup(global->amqp);
	ast_string_field_free_memory(global);
}

static struct cdr_amqp_global_conf *conf_global_create(void)
{
	RAII_VAR(struct cdr_amqp_global_conf *, global, NULL, ao2_cleanup);

	global = ao2_alloc(sizeof(*global), conf_global_dtor);
	if (!global) {
		return NULL;
	}

	if (ast_string_field_init(global, 64) != 0) {
		return NULL;
	}

	aco_set_defaults(&global_option, "global", global);

	return ao2_bump(global);
}

/*! \brief The conf file that's processed for the module. */
static struct aco_file conf_file = {
	/*! The config file name. */
	.filename = CONF_FILENAME,
	/*! The mapping object types to be processed. */
	.types = ACO_TYPES(&global_option),
};

static void conf_dtor(void *obj)
{
	struct cdr_amqp_conf *conf = obj;

	ao2_cleanup(conf->global);
}

static void *conf_alloc(void)
{
	RAII_VAR(struct cdr_amqp_conf *, conf, NULL, ao2_cleanup);

	conf = ao2_alloc_options(sizeof(*conf), conf_dtor,
		AO2_ALLOC_OPT_LOCK_NOLOCK);
	if (!conf) {
		return NULL;
	}

	conf->global = conf_global_create();
	if (!conf->global) {
		return NULL;
	}

	return ao2_bump(conf);
}

static int setup_amqp(void);

CONFIG_INFO_STANDARD(cfg_info, confs, conf_alloc,
	.files = ACO_FILES(&conf_file),
	.pre_apply_config = setup_amqp,
);

static int setup_amqp(void)
{
	struct cdr_amqp_conf *conf = aco_pending_config(&cfg_info);

	if (!conf) {
		return 0;
	}

	if (!conf->global) {
		ast_log(LOG_ERROR, "Invalid cdr_amqp.conf\n");
		return -1;
	}

	/* Refresh the AMQP connection */
	ao2_cleanup(conf->global->amqp);
	conf->global->amqp = ast_amqp_get_connection(conf->global->connection);

	if (!conf->global->amqp) {
		ast_log(LOG_ERROR, "Could not get AMQP connection %s\n",
			conf->global->connection);
		return -1;
	}

	return 0;
}

/*!
 * \brief CDR handler for AMQP.
 *
 * \param cdr CDR to log.
 * \return 0 on success.
 * \return -1 on error.
 */
static int amqp_cdr_log(struct ast_cdr *cdr)
{
	RAII_VAR(struct cdr_amqp_conf *, conf, NULL, ao2_cleanup);
	RAII_VAR(struct ast_json *, json, NULL, ast_json_unref);
	RAII_VAR(char *, str, NULL, ast_json_free);
	RAII_VAR(struct ast_json *, disposition, NULL, ast_json_unref);
	int res;
	amqp_basic_properties_t props = {
		._flags = AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_CONTENT_TYPE_FLAG,
		.delivery_mode = 2, /* persistent delivery mode */
		.content_type = amqp_cstring_bytes("application/json")
	};

	conf = ao2_global_obj_ref(confs);

	ast_assert(conf && conf->global && conf->global->amqp);

	json = ast_json_pack("{"
		/* clid, src, dst, dcontext */
		"s: s, s: s, s: s, s: s,"
		/* channel, dstchannel, lastapp, lastdata */
		"s: s, s: s, s: s, s: s,"
		/* start, answer, end, duration */
		"s: o, s: o, s: o, s: i"
		/* billsec, disposition, accountcode, amaflags */
		"s: i, s: s, s: s, s: s"
		/* peeraccount, linkedid */
		"s: s, s: s }",

		"clid", cdr->clid,
		"src", cdr->src,
		"dst", cdr->dst,
		"dcontext", cdr->dcontext,

		"channel", cdr->channel,
		"dstchannel", cdr->dstchannel,
		"lastapp", cdr->lastapp,
		"lastdata", cdr->lastdata,

		"start", ast_json_timeval(cdr->start, NULL),
		"answer", ast_json_timeval(cdr->answer, NULL),
		"end", ast_json_timeval(cdr->end, NULL),
		"durationsec", cdr->duration,

		"billsec", cdr->billsec,
		"disposition", ast_cdr_disp2str(cdr->disposition),
		"accountcode", cdr->accountcode,
		"amaflags", ast_channel_amaflags2string(cdr->amaflags),

		"peeraccount", cdr->peeraccount,
		"linkedid", cdr->linkedid);
	if (!json) {
		return -1;
	}

	/* Set optional fields */
	if (conf->global->loguniqueid) {
		ast_json_object_set(json,
			"uniqueid", ast_json_string_create(cdr->uniqueid));
	}

	if (conf->global->loguserfield) {
		ast_json_object_set(json,
			"userfield", ast_json_string_create(cdr->userfield));
	}

	/* Dump the JSON to a string for publication */
	str = ast_json_dump_string(json);
	if (!str) {
		ast_log(LOG_ERROR, "Failed to build string from JSON\n");
		return -1;
	}

	res = ast_amqp_basic_publish(conf->global->amqp,
		amqp_cstring_bytes(conf->global->exchange),
		amqp_cstring_bytes(conf->global->queue),
		0, /* mandatory; don't return unsendable messages */
		0, /* immediate; allow messages to be queued */
		&props,
		amqp_cstring_bytes(str));

	if (res != 0) {
		ast_log(LOG_ERROR, "Error publishing CDR to AMQP\n");
		return -1;
	}

	return 0;
}


static int load_config(int reload)
{
	RAII_VAR(struct cdr_amqp_conf *, conf, NULL, ao2_cleanup);
	RAII_VAR(struct ast_amqp_connection *, amqp, NULL, ao2_cleanup);

	switch (aco_process_config(&cfg_info, reload)) {
	case ACO_PROCESS_ERROR:
		return -1;
	case ACO_PROCESS_OK:
	case ACO_PROCESS_UNCHANGED:
		break;
	}

	conf = ao2_global_obj_ref(confs);
	if (!conf || !conf->global) {
		ast_log(LOG_ERROR, "Error obtaining config from cdr_amqp.conf\n");
		return -1;
	}

	return 0;
}

static int load_module(void)
{
	if (aco_info_init(&cfg_info) != 0) {
		ast_log(LOG_ERROR, "Failed to initialize config");
		aco_info_destroy(&cfg_info);
		return -1;
	}

	aco_option_register(&cfg_info, "loguniqueid", ACO_EXACT,
		global_options, "no", OPT_BOOL_T, 1,
		FLDSET(struct cdr_amqp_global_conf, loguniqueid));
	aco_option_register(&cfg_info, "loguserfield", ACO_EXACT,
		global_options, "no", OPT_BOOL_T, 1,
		FLDSET(struct cdr_amqp_global_conf, loguserfield));
	aco_option_register(&cfg_info, "connection", ACO_EXACT,
		global_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cdr_amqp_global_conf, connection));
	aco_option_register(&cfg_info, "queue", ACO_EXACT,
		global_options, "asterisk_cdr", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cdr_amqp_global_conf, queue));
	aco_option_register(&cfg_info, "exchange", ACO_EXACT,
		global_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cdr_amqp_global_conf, exchange));

	if (load_config(0) != 0) {
		ast_log(LOG_WARNING, "Configuration failed to load\n");
		return AST_MODULE_LOAD_DECLINE;
	}

	if (ast_cdr_register(CDR_NAME, ast_module_info->description, amqp_cdr_log) != 0) {
		ast_log(LOG_ERROR, "Could not register CDR backend\n");
		return AST_MODULE_LOAD_FAILURE;
	}

	ast_log(LOG_NOTICE, "CDR AMQP logging enabled\n");
	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	aco_info_destroy(&cfg_info);
	ao2_global_obj_release(confs);
	if (ast_cdr_unregister(CDR_NAME) != 0) {
		return -1;
	}

	return 0;
}

static int reload_module(void)
{
	return load_config(1);
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_LOAD_ORDER, "AMQP CDR Backend",
		.support_level = AST_MODULE_SUPPORT_CORE,
		.load = load_module,
		.unload = unload_module,
		.reload = reload_module,
		.load_pri = AST_MODPRI_CDR_DRIVER,
	);