class CollectionManager {
  constructor (db, options = {}) {
    const defaults = {
      resumeTokenCollection: 'resumeTokens',
      watch: {},
      handlers: {},
      logger: console,
    };

    this.db = db;
    this.options = Object.assign({}, defaults, options);
    this.model = this.db.collection(this.options.collection);
    this.resumeTokenModel = this.db.collection(this.options.resumeTokenCollection);
    this.logger = this.options.logger;
    this._stop = false;
  }

  async close () {
    if (this.changeStream) {
      this._stop = true;

      return this.changeStream.close();
    }
  }
  async watch () {
    // Connected state only
    if (this.db.readyState !== 1) return;

    this.db.once('disconnected', this._onConnDisconnected.bind(this));
    const token = await this.getResumeToken();

    this.changeStream = this.model.watch(Object.assign({ resumeAfter: token }, this.options.watch));

    this.changeStream.on('change', this._onChange.bind(this));
    this.changeStream.on('error', this._onError.bind(this));
    this.changeStream.on('end', this._onEnd.bind(this));
    this.changeStream.on('close', this._onClose.bind(this));

    this._stop = false;
    this.logger.info(`${this.options.collection} is watched`);
  }

  async _onChange (data) {
    if (!(this.options.handlers && this.options.handlers.onChange)) return;

    const self = this;
    this.changeStream.pause();
    try {
      await self.options.handlers.onChange(data);
      await self.writeResumeToken(data._id);
    } catch (error) {
      this.logger.error(`On change error`, error);
    } finally {
      self.changeStream.resume();
    }
  }

  async _onError (error) {
    // Database does not exist
    if (error.code === 26) {
      this._stop = true;
    }

    if (this.options.handlers && this.options.handlers.onError) {
      await this.options.handlers.onError(error);
    }
  }

  async _onEnd () {
    if (this.options.handlers && this.options.handlers.onEnd) {
      await this.options.handlers.onEnd();
    }
  }

  async _onClose () {
    if (this.options.handlers && this.options.handlers.onClose) {
      await this.options.handlers.onClose();
    }

    this.rewatch();
  }

  async _onConnDisconnected () {
    const self = this;

    this.removeAllListeners();
    await this.close();

    this.logger.info('DB is disconnected. Wait for connection');
    this.db.once('connected', () => {
      self.logger.info('DB is connected. Rewatch');
      self.rewatch();
    });
  }

  rewatch () {
    if (this._stop) {
      this.logger.info('Found stop flag. No need to rewatch');
      return;
    }

    this.logger.info('rewatch...');
    this.removeAllListeners();
    this.watch();
  }

  removeAllListeners () {
    if (this.changeStream) {
      this.changeStream.removeAllListeners();
      delete this.changeStream;
    }
  }

  async writeResumeToken (resumeToken) {
    return this.resumeTokenModel.findOneAndUpdate(
      { collection: this.options.collection },
      { $set: { resumeToken, updatedAt: new Date() }},
      { upsert: true, new: false }
    );
  }

  async getResumeToken () {
    const data = await this.resumeTokenModel.findOne({ collection: this.options.collection });
    return data ? data.resumeToken : null;
  }
}

module.exports = CollectionManager;
